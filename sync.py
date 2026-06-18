"""Sync orchestrator for Close CRM to Supabase synchronization.

Implements:
- Incremental sync (watermark-based)
- Full re-sync (complete refresh)
- LeadMaggy per-lead resolution
- Activity routing to correct tables
"""

import logging
import re
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID

from close_client import CloseClient
from supabase_client import SupabaseClient
from partner_matcher import PartnerMatcher
from mappers import (
    map_lead,
    map_custom_activity,
    map_partner_referral,
    map_partner_upload,
    map_lead_magnet,
)
from config import Config

logger = logging.getLogger(__name__)


def generate_slug(name: str) -> str:
    """
    Generate a URL-safe slug from a partner name.
    
    Args:
        name: Partner name (e.g., "Property Finance Choices Ltd")
        
    Returns:
        Slug (e.g., "property_finance_choices_ltd")
    """
    # Lowercase
    slug = name.lower()
    
    # Remove common suffixes
    suffixes = [" ltd", " limited", " plc", " llp", " inc", " llc", " corp", " corporation"]
    for suffix in suffixes:
        if slug.endswith(suffix):
            slug = slug[:-len(suffix)]
    
    # Replace non-alphanumeric with underscore
    slug = re.sub(r"[^a-z0-9]+", "_", slug)
    
    # Remove leading/trailing underscores
    slug = slug.strip("_")
    
    # Truncate if too long (max 63 chars for safety)
    if len(slug) > 63:
        slug = slug[:63].rstrip("_")
    
    return slug


def generate_partner_email(partner_name: str, slug: str) -> str:
    """
    Generate an email address for a partner auth user.
    
    Args:
        partner_name: Partner name
        slug: Partner slug
        
    Returns:
        Email (e.g., "partner+property_finance@close-sync.local")
    """
    return f"partner+{slug}@close-sync.local"


class SyncStats:
    """Track sync statistics."""
    
    def __init__(self):
        self.fetched = 0
        self.inserted = 0
        self.updated = 0
        self.skipped = 0
        self.errors = 0
        self.error_details = []
    
    def add_error(self, error: str):
        self.errors += 1
        self.error_details.append(error)
        if len(self.error_details) > 100:
            self.error_details = self.error_details[-100:]


class SyncOrchestrator:
    """Orchestrates the sync process between Close and Supabase."""

    def __init__(self, close: CloseClient, supabase: SupabaseClient):
        self.close = close
        self.supabase = supabase
        self.partner_matcher: Optional[PartnerMatcher] = None

    def _init_partner_matcher(self):
        """Initialize partner matcher with current partners from DB."""
        if self.partner_matcher is None:
            partners = self.supabase.get_all_partners()
            self.partner_matcher = PartnerMatcher(partners)

    # -------------------------------------------------------------------------
    # Leads Sync
    # -------------------------------------------------------------------------

    def sync_leads(self, mode: str = "incremental", max_leads: int = None) -> SyncStats:
        """
        Sync leads from Close smart view to Supabase.
        
        Args:
            mode: "incremental" or "full"
            max_leads: Optional limit on number of leads to process (for testing)
            
        Returns:
            SyncStats with counts
        """
        stats = SyncStats()
        sync_name = f"leads_{mode}"
        
        # Capture run_started_at BEFORE fetching (Invariant B)
        run_started_at = datetime.now(timezone.utc)
        
        # Get watermark for incremental
        date_filter = None
        if mode == "incremental":
            date_filter = self.supabase.get_watermark("leads")
            logger.info(f"Leads incremental sync from watermark: {date_filter}")
        else:
            logger.info("Leads full sync (no date filter)")
        
        # Initialize partner matcher
        self._init_partner_matcher()
        
        # Create sync run record
        run_id = self.supabase.create_sync_run(
            sync_name=sync_name,
            target_table="leads",
        )
        
        try:
            # Batch leads for bulk upsert
            batch = []
            batch_size = 100
            
            for lead in self.close.get_leads_from_smart_view(
                smart_view_id=Config.LEAD_SOURCE_SMART_VIEW_ID,
                date_updated_gte=date_filter,
            ):
                stats.fetched += 1
                
                # Check max_leads limit for testing
                if max_leads and stats.fetched > max_leads:
                    logger.info(f"Reached max_leads limit ({max_leads}), stopping")
                    break
                
                try:
                    # Resolve partner IDs
                    partner_id, secondary_partner_id = self.partner_matcher.match_from_lead(lead)
                    
                    # Map to database schema
                    mapped = map_lead(lead, partner_id, secondary_partner_id)
                    batch.append(mapped)
                    
                    # Upsert in batches (deduplicate by close_lead_id first)
                    if len(batch) >= batch_size:
                        # Deduplicate batch - keep last occurrence (most recent data)
                        seen = {}
                        for lead in batch:
                            seen[lead["close_lead_id"]] = lead
                        deduped_batch = list(seen.values())
                        
                        inserted, updated = self.supabase.upsert_leads(deduped_batch)
                        stats.inserted += inserted
                        stats.updated += updated
                        batch = []
                    
                except Exception as e:
                    stats.add_error(f"Lead {lead.get('id')}: {str(e)}")
                    logger.error(f"Error processing lead {lead.get('id')}: {e}")
            
            # Upsert remaining batch (deduplicate by close_lead_id first)
            if batch:
                # Deduplicate batch - keep last occurrence (most recent data)
                seen = {}
                for lead in batch:
                    seen[lead["close_lead_id"]] = lead
                batch = list(seen.values())
                
                inserted, updated = self.supabase.upsert_leads(batch)
                stats.inserted += inserted
                stats.updated += updated
            
            # Update watermark on success (Invariant B - anchored to run_started_at)
            self.supabase.update_watermark("leads", run_started_at)
            
            # Update sync run
            self.supabase.update_sync_run(
                run_id=run_id,
                status="completed",
                fetched_count=stats.fetched,
                inserted_count=stats.inserted,
                updated_count=stats.updated,
                error_count=stats.errors,
                error_details=stats.error_details if stats.error_details else None,
            )
            
            logger.info(f"Leads sync completed: fetched={stats.fetched}, updated={stats.updated}, errors={stats.errors}")
            
        except Exception as e:
            logger.error(f"Leads sync failed: {e}")
            stats.add_error(str(e))
            
            # Mark run as failed - DO NOT advance watermark
            self.supabase.update_sync_run(
                run_id=run_id,
                status="failed",
                fetched_count=stats.fetched,
                inserted_count=stats.inserted,
                updated_count=stats.updated,
                error_count=stats.errors,
                error_details=stats.error_details,
            )
            raise
        
        return stats

    # -------------------------------------------------------------------------
    # LeadMaggy Sync (per-lead path - SOLE writer of lead_magnet table)
    # -------------------------------------------------------------------------

    def sync_lead_magnets(self, mode: str = "incremental", max_leads: int = None) -> SyncStats:
        """
        Sync LeadMaggy activities via per-lead resolution.
        
        This is the SOLE writer to the lead_magnet table.
        The activities sync MUST skip LeadMaggy type IDs.
        
        Args:
            mode: "incremental" or "full"
            max_leads: Optional limit on number of leads to process (for testing)
            
        Returns:
            SyncStats with counts
        """
        stats = SyncStats()
        sync_name = f"lead_magnet_{mode}"
        
        run_started_at = datetime.now(timezone.utc)
        
        # Get watermark
        date_filter = None
        if mode == "incremental":
            date_filter = self.supabase.get_watermark("lead_magnet")
            logger.info(f"LeadMagnet incremental sync from watermark: {date_filter}")
        
        # Initialize partner matcher
        self._init_partner_matcher()
        
        run_id = self.supabase.create_sync_run(
            sync_name=sync_name,
            target_table="lead_magnet",
        )
        
        try:
            # For LeadMaggy, we need to iterate through leads and fetch their latest activity
            # In incremental mode, we only process leads updated since watermark
            for lead in self.close.get_leads_from_smart_view(
                smart_view_id=Config.LEAD_SOURCE_SMART_VIEW_ID,
                date_updated_gte=date_filter,
            ):
                lead_id = lead.get("id")
                stats.fetched += 1
                
                # Check max_leads limit for testing
                if max_leads and stats.fetched > max_leads:
                    logger.info(f"Reached max_leads limit ({max_leads}), stopping")
                    break
                
                try:
                    # Get latest LeadMaggy activity for this lead
                    activity = self.close.get_latest_lead_maggy_activity(lead_id)
                    
                    if not activity:
                        stats.skipped += 1
                        continue
                    
                    # Resolve partner_id from the lead
                    partner_id, _ = self.partner_matcher.match_from_lead(lead)
                    
                    # First upsert to custom_activities to get/create UUID
                    custom_activity_mapped = map_custom_activity(activity, partner_id)
                    self.supabase.upsert_custom_activities([custom_activity_mapped])
                    
                    # Get the UUID
                    custom_activity_uuid = self.supabase.get_custom_activity_uuid(activity.get("id"))
                    
                    if not custom_activity_uuid:
                        stats.add_error(f"Could not get UUID for activity {activity.get('id')}")
                        continue
                    
                    # Map and upsert lead magnet
                    lead_magnet_mapped = map_lead_magnet(activity, custom_activity_uuid)
                    inserted, updated = self.supabase.upsert_lead_magnets([lead_magnet_mapped])
                    stats.inserted += inserted
                    stats.updated += updated
                    
                except Exception as e:
                    stats.add_error(f"Lead {lead_id} LeadMaggy: {str(e)}")
                    logger.error(f"Error processing LeadMaggy for lead {lead_id}: {e}")
            
            # Update watermark on success
            self.supabase.update_watermark("lead_magnet", run_started_at)
            
            self.supabase.update_sync_run(
                run_id=run_id,
                status="completed",
                fetched_count=stats.fetched,
                inserted_count=stats.inserted,
                updated_count=stats.updated,
                skipped_count=stats.skipped,
                error_count=stats.errors,
                error_details=stats.error_details if stats.error_details else None,
            )
            
            logger.info(f"LeadMagnet sync completed: fetched={stats.fetched}, updated={stats.updated}, skipped={stats.skipped}")
            
        except Exception as e:
            logger.error(f"LeadMagnet sync failed: {e}")
            stats.add_error(str(e))
            
            self.supabase.update_sync_run(
                run_id=run_id,
                status="failed",
                fetched_count=stats.fetched,
                error_count=stats.errors,
                error_details=stats.error_details,
            )
            raise
        
        return stats

    # -------------------------------------------------------------------------
    # Activities Sync (partner_referral, partner_upload only - NOT LeadMaggy)
    # -------------------------------------------------------------------------

    def sync_activities(self, mode: str = "incremental") -> SyncStats:
        """
        Sync partner activities from Close to Supabase using smart view filtering.
        
        Routes activities to correct tables:
        - actitype_1CKUCsig (GEN1. Referral Upload) -> partner_referral
        - actitype_0PpighCx (API - Referral Upload) -> partner_referral
        - actitype_5rvWuLY9 (GEN2. New Partner Upload) -> partner_upload
        
        Uses saved smart views to efficiently fetch only relevant leads/activities
        instead of scanning all activities globally.
        
        IMPORTANT: LeadMaggy type IDs are SKIPPED - handled by sync_lead_magnets only.
        
        Args:
            mode: "incremental" or "full"
            
        Returns:
            SyncStats with counts
        """
        stats = SyncStats()
        sync_name = f"activities_{mode}"
        
        run_started_at = datetime.now(timezone.utc)
        
        # Get watermark
        date_filter = None
        if mode == "incremental":
            date_filter = self.supabase.get_watermark("activities")
            logger.info(f"Activities incremental sync from watermark: {date_filter}")
        else:
            logger.info("Activities full sync (no date filter)")
        
        run_id = self.supabase.create_sync_run(
            sync_name=sync_name,
            target_table="custom_activities,partner_referral,partner_upload",
        )
        
        try:
            self._init_partner_matcher()
            
            # Store batches: (custom_activity_mapped, original_activity) tuples
            pending_activities = []
            batch_size = 100
            
            # Track activities we've already seen (to avoid duplicates)
            seen_activity_ids = set()
            
            def flush_batch(batch: list) -> tuple[int, int]:
                """Upsert activities to custom_activities and route to specific tables."""
                if not batch:
                    return 0, 0
                
                inserted_total = 0
                updated_total = 0
                
                # First, upsert all to custom_activities
                custom_activity_records = [item[0] for item in batch]
                self.supabase.upsert_custom_activities(custom_activity_records)
                
                # Now route to specific tables
                referral_records = []
                upload_records = []
                
                for custom_mapped, original_activity in batch:
                    close_id = original_activity.get("id")
                    activity_type = original_activity.get("custom_activity_type_id")
                    
                    # Get UUID from custom_activities
                    uuid = self.supabase.get_custom_activity_uuid(close_id)
                    if not uuid:
                        stats.add_error(f"No UUID for activity {close_id}")
                        continue
                    
                    # Route to correct table based on activity type
                    if activity_type in Config.PARTNER_REFERRAL_TYPE_IDS:
                        mapped = map_partner_referral(original_activity, uuid)
                        referral_records.append(mapped)
                    elif activity_type == Config.PARTNER_UPLOAD_TYPE_ID:
                        mapped = map_partner_upload(original_activity, uuid)
                        upload_records.append(mapped)
                
                # Upsert to routing tables
                if referral_records:
                    ins, upd = self.supabase.upsert_partner_referrals(referral_records)
                    inserted_total += ins
                    updated_total += upd
                
                if upload_records:
                    ins, upd = self.supabase.upsert_partner_uploads(upload_records)
                    inserted_total += ins
                    updated_total += upd
                
                return inserted_total, updated_total
            
            # Use smart view approach - fetch activities via activity-specific smart views
            # This only processes leads that actually have activities of each type
            logger.info("Using smart view approach for activity sync")
            
            for activity, lead in self.close.get_activities_via_smart_views(
                activity_type_smart_views=Config.ACTIVITY_SMART_VIEW_IDS,
                date_updated_gte=date_filter,
            ):
                activity_id = activity.get("id")
                
                # Skip duplicates (same activity could appear if lead matches multiple views)
                if activity_id in seen_activity_ids:
                    continue
                seen_activity_ids.add(activity_id)
                
                stats.fetched += 1
                
                try:
                    # Resolve partner_id from the lead (already fetched - no extra API call!)
                    partner_id, _ = self.partner_matcher.match_from_lead(lead)
                    
                    # Map custom activity with partner_id
                    custom_activity_mapped = map_custom_activity(activity, partner_id)
                    
                    # Store both mapped record and original for routing
                    pending_activities.append((custom_activity_mapped, activity))
                    
                    # Flush batch if needed
                    if len(pending_activities) >= batch_size:
                        inserted, updated = flush_batch(pending_activities)
                        stats.inserted += inserted
                        stats.updated += updated
                        pending_activities = []
                    
                except Exception as e:
                    stats.add_error(f"Activity {activity_id}: {str(e)}")
                    logger.error(f"Error processing activity {activity_id}: {e}")
                    continue
            
            # Flush remaining batch
            if pending_activities:
                inserted, updated = flush_batch(pending_activities)
                stats.inserted += inserted
                stats.updated += updated
            
            # Update watermark
            self.supabase.update_watermark("activities", run_started_at)
            
            self.supabase.update_sync_run(
                run_id=run_id,
                status="completed",
                fetched_count=stats.fetched,
                inserted_count=stats.inserted,
                updated_count=stats.updated,
                error_count=stats.errors,
                error_details=stats.error_details if stats.error_details else None,
            )
            
            logger.info(f"Activities sync completed: fetched={stats.fetched}, inserted={stats.inserted}, updated={stats.updated}")
            
        except Exception as e:
            logger.error(f"Activities sync failed: {e}")
            stats.add_error(str(e))
            
            self.supabase.update_sync_run(
                run_id=run_id,
                status="failed",
                fetched_count=stats.fetched,
                error_count=stats.errors,
                error_details=stats.error_details,
            )
            raise
        
        return stats

    # -------------------------------------------------------------------------
    # Partners Sync
    # -------------------------------------------------------------------------

    def sync_partners(self) -> SyncStats:
        """
        Sync partner status from Close smart view to Supabase.
        
        - Fetches partners from the Close smart view (filter-based, not by lead_id)
        - Sets lead_id from Close only when missing (matched by name); skips if already set
        - Deactivates partners not in Close smart view (except platform admins)
        - Re-activates partners that return to the smart view (except platform admins)
        - Reports new partners (cannot auto-add - require user accounts)
        - Updates paid_partner when dealsheet rows have partner_comms_total_amount > 0
          (linked via lead_id, close_lead_id, partner_introducer, or company/lead name)
        
        Returns:
            SyncStats with counts
        """
        stats = SyncStats()
        sync_name = "partners"
        
        run_id = self.supabase.create_sync_run(
            sync_name=sync_name,
            target_table="partners",
        )
        
        try:
            # 1. Fetch partners from Close smart view
            logger.info(f"Fetching partners from Close smart view: {Config.PARTNERS_SMART_VIEW_ID}")
            close_partners = []
            for lead in self.close.get_leads_from_smart_view(Config.PARTNERS_SMART_VIEW_ID):
                name = lead.get("name") or lead.get("display_name")
                if name:
                    close_partners.append({
                        "close_lead_id": lead.get("id"),
                        "name": name.strip(),
                    })
                    stats.fetched += 1
            
            close_partner_map = {p["name"].lower().strip(): p for p in close_partners}
            logger.info(f"Found {len(close_partners)} active partners in Close")
            
            # 2. Fetch all partners from Supabase (excluding soft-deleted)
            result = self.supabase.client.table("partners").select(
                "uuid, partner_name, is_active, lead_id, is_platform_admin"
            ).eq("is_deleted", False).execute()
            db_partners = result.data or []
            logger.info(f"Found {len(db_partners)} partners in database")
            
            db_partner_map = {p["partner_name"].lower().strip(): p for p in db_partners}
            
            # 3. Identify changes
            to_update = []  # Existing partners to update (lead_id, activate)
            to_deactivate = []  # Partners not in Close
            new_partners = []  # Partners in Close but not in DB
            
            # Partners in Close - check if they exist and need updates
            for cp in close_partners:
                name_lower = cp["name"].lower().strip()
                if name_lower in db_partner_map:
                    db_p = db_partner_map[name_lower]
                    needs_update = False
                    updates = {"uuid": db_p["uuid"]}

                    # Only set lead_id when missing — clear stale values manually via SQL
                    if not db_p.get("lead_id"):
                        updates["lead_id"] = cp["close_lead_id"]
                        needs_update = True

                    if not db_p["is_active"] and not db_p.get("is_platform_admin"):
                        updates["is_active"] = True
                        needs_update = True

                    if needs_update:
                        to_update.append(updates)
                else:
                    new_partners.append(cp["name"])
            
            # Partners in DB that are not in Close (deactivate; skip platform admins)
            for db_p in db_partners:
                name_lower = db_p["partner_name"].lower().strip()
                if (
                    name_lower not in close_partner_map
                    and db_p["is_active"]
                    and not db_p.get("is_platform_admin")
                ):
                    to_deactivate.append({
                        "uuid": db_p["uuid"],
                        "name": db_p["partner_name"],
                    })
            
            # 4. Apply updates
            for update in to_update:
                uuid = update.pop("uuid")
                self.supabase.client.table("partners").update(update).eq("uuid", uuid).execute()
                stats.updated += 1
            
            if to_update:
                logger.info(f"Updated {len(to_update)} partners (lead_id/is_active)")
            
            # 5. Deactivate partners not in Close
            for p in to_deactivate:
                self.supabase.client.table("partners").update({
                    "is_active": False,
                }).eq("uuid", p["uuid"]).execute()
                stats.updated += 1
                logger.debug(f"Deactivated: {p['name']}")
            
            if to_deactivate:
                logger.info(f"Deactivated {len(to_deactivate)} partners not in Close")
            
            # 6. Insert new partners (create auth users + partner records)
            if new_partners:
                logger.info(f"Inserting {len(new_partners)} new partners from Close")
                
                for partner_name in new_partners:
                    try:
                        # Find the Close lead_id for this partner
                        cp = close_partner_map[partner_name.lower().strip()]
                        close_lead_id = cp["close_lead_id"]
                        
                        # Generate slug
                        slug = generate_slug(partner_name)
                        
                        # Check if slug already exists (collision)
                        existing_slug = self.supabase.client.table("partners").select("slug").eq("slug", slug).execute()
                        if existing_slug.data:
                            # Append a unique suffix
                            import uuid
                            slug = f"{slug}_{uuid.uuid4().hex[:6]}"
                            logger.debug(f"Slug collision, using: {slug}")
                        
                        # Generate email
                        email = generate_partner_email(partner_name, slug)
                        
                        # Create auth user
                        user_id = self.supabase.create_partner_auth_user(partner_name, email)
                        if not user_id:
                            stats.add_error(f"Failed to create auth user for '{partner_name}'")
                            stats.skipped += 1
                            continue
                        
                        # Insert partner
                        partner_data = {
                            "user_id": user_id,
                            "partner_name": partner_name,
                            "slug": slug,
                            "lead_id": close_lead_id,
                            "is_active": True,
                        }
                        
                        if self.supabase.insert_partner(partner_data):
                            stats.inserted += 1
                        else:
                            stats.add_error(f"Failed to insert partner '{partner_name}'")
                            stats.skipped += 1
                            
                    except Exception as e:
                        stats.add_error(f"Error creating partner '{partner_name}': {str(e)}")
                        logger.error(f"Error creating partner '{partner_name}': {e}")
                        stats.skipped += 1
                
                if stats.inserted > 0:
                    logger.info(f"Successfully inserted {stats.inserted} new partners")
                if stats.skipped > 0:
                    logger.warning(f"Skipped {stats.skipped} partners due to errors")
            
            # 7. Update paid_partner status based on dealsheet funding data
            logger.info("=== Updating paid_partner status from dealsheet data ===")
            try:
                funded_partner_uuids = self.supabase.get_funded_partner_uuids()
                
                # Refresh db_partners list to include any newly inserted partners
                current_partners = self.supabase.client.table("partners").select(
                    "uuid, partner_name, paid_partner, is_platform_admin"
                ).eq("is_deleted", False).execute()
                
                paid_updated = 0
                for partner in current_partners.data or []:
                    uuid = partner["uuid"]
                    current_paid = partner.get("paid_partner")
                    should_be_paid = uuid in funded_partner_uuids
                    
                    # Skip platform admin partners for paid_partner logic
                    if partner.get("is_platform_admin"):
                        continue
                    
                    # Only update if status changed
                    if current_paid != should_be_paid:
                        self.supabase.client.table("partners").update({
                            "paid_partner": should_be_paid
                        }).eq("uuid", uuid).execute()
                        paid_updated += 1
                        logger.debug(
                            f"Updated paid_partner for '{partner['partner_name']}': "
                            f"{current_paid} → {should_be_paid}"
                        )
                
                if paid_updated > 0:
                    logger.info(f"Updated paid_partner for {paid_updated} partners based on dealsheet data")
                    stats.updated += paid_updated
                else:
                    logger.info("No paid_partner changes needed")
                    
            except Exception as e:
                logger.error(f"Failed to update paid_partner status: {e}")
                stats.add_error(f"paid_partner update failed: {str(e)}")
            
            # Update sync run
            self.supabase.update_sync_run(
                run_id=run_id,
                status="completed",
                fetched_count=stats.fetched,
                inserted_count=stats.inserted,
                updated_count=stats.updated,
                skipped_count=stats.skipped,
                error_count=stats.errors,
                error_details=stats.error_details if stats.error_details else None,
            )
            
            logger.info(
                f"Partners sync completed: fetched={stats.fetched}, "
                f"updated={stats.updated}, skipped={stats.skipped} (need manual setup)"
            )
            
        except Exception as e:
            logger.error(f"Partners sync failed: {e}")
            stats.add_error(str(e))
            
            self.supabase.update_sync_run(
                run_id=run_id,
                status="failed",
                fetched_count=stats.fetched,
                error_count=stats.errors,
                error_details=stats.error_details,
            )
            raise
        
        return stats


def run_sync(
    mode: str = "incremental",
    max_leads: int = None,
    phase: str = "all",
    skip_lock: bool = False,
):
    """
    Run the sync process.
    
    Args:
        mode: "incremental" or "full"
        max_leads: Optional limit on number of leads to process (for testing)
        phase: "all", "partners", "leads", "lead_magnets", or "activities"
        skip_lock: If True, skip advisory lock check (for testing only)
    """
    logger.info(
        f"Starting {mode} sync"
        f"{f' (max_leads={max_leads})' if max_leads else ''}"
        f" [phase={phase}]"
        f"{' [SKIP LOCK]' if skip_lock else ''}"
    )
    
    with CloseClient() as close, SupabaseClient() as supabase:
        # Try to acquire advisory lock (unless skipped for testing)
        if not skip_lock and not supabase.try_acquire_lock():
            supabase.log_skipped_run(f"sync_{mode}", "lock_not_acquired")
            logger.warning("Sync skipped - another sync is running")
            return
        
        if skip_lock:
            logger.warning("Advisory lock check SKIPPED (testing mode)")
        
        try:
            orchestrator = SyncOrchestrator(close, supabase)
            
            # Partners sync (runs first if included - updates active status)
            if phase in ("all", "partners"):
                logger.info("=== Starting partners sync ===")
                partners_stats = orchestrator.sync_partners()
                logger.info(
                    f"Partners: fetched={partners_stats.fetched}, "
                    f"updated={partners_stats.updated}, skipped={partners_stats.skipped}"
                )
            
            if phase in ("all", "leads"):
                logger.info("=== Starting leads sync ===")
                leads_stats = orchestrator.sync_leads(mode, max_leads)
                logger.info(
                    f"Leads: fetched={leads_stats.fetched}, updated={leads_stats.updated}"
                )
            
            if phase in ("all", "lead_magnets"):
                logger.info("=== Starting lead_magnet sync ===")
                magnet_stats = orchestrator.sync_lead_magnets(mode, max_leads)
                logger.info(
                    f"LeadMagnet: fetched={magnet_stats.fetched}, updated={magnet_stats.updated}"
                )
            
            if phase in ("all", "activities"):
                logger.info("=== Starting activities sync ===")
                activity_stats = orchestrator.sync_activities(mode)
                logger.info(
                    f"Activities: fetched={activity_stats.fetched}, updated={activity_stats.updated}"
                )
            
            logger.info(f"=== {mode.upper()} sync completed (phase={phase}) ===")
            
        finally:
            supabase.release_lock()
