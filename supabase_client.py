"""Supabase client with upserts, watermarks, and advisory locking."""

import logging
import re
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import UUID

import psycopg2
from supabase import create_client, Client

from config import Config
from mappers import normalize_company
from lead_matcher import LeadMatcher, normalize_entity_name

logger = logging.getLogger(__name__)

ADVISORY_LOCK_ID = 1  # Global lock ID shared by all sync modes
_COMMISSION_ZERO = frozenset({"0", "0.0", "0.00", "£0", "£0.00"})
_DEALSHEET_PAGE_SIZE = 1000
_LEAD_PAGE_SIZE = 1000


def _parse_commission_amount(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text in _COMMISSION_ZERO:
        return None
    cleaned = re.sub(r"[^0-9.\-]", "", text)
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _has_partner_commission(value: Any) -> bool:
    amount = _parse_commission_amount(value)
    return amount is not None and amount > 0


def _normalize_entity_name(name: str) -> str:
    return normalize_entity_name(name)


class SupabaseClient:
    """Client for Supabase operations including upserts and watermark management."""

    def __init__(self):
        self.client: Client = create_client(
            Config.SUPABASE_URL,
            Config.SUPABASE_SERVICE_ROLE_KEY,
        )
        self._pg_conn: Optional[psycopg2.extensions.connection] = None
        self._lock_acquired = False

    def close(self):
        if self._pg_conn:
            self._pg_conn.close()
            self._pg_conn = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def _get_pg_connection(self) -> psycopg2.extensions.connection:
        """Get or create a direct PostgreSQL connection for advisory locks."""
        if self._pg_conn is None or self._pg_conn.closed:
            try:
                self._pg_conn = psycopg2.connect(Config.get_db_connection_string())
            except ValueError as e:
                logger.error(str(e))
                raise
            self._pg_conn.autocommit = True
        return self._pg_conn

    def try_acquire_lock(self) -> bool:
        """
        Try to acquire the global advisory lock.
        
        Returns True if lock acquired, False if another process holds it.
        The lock is tied to the connection - it auto-releases if the connection drops.
        """
        conn = self._get_pg_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (ADVISORY_LOCK_ID,))
            result = cur.fetchone()
            self._lock_acquired = result[0] if result else False
            
        if self._lock_acquired:
            logger.info("Advisory lock acquired")
        else:
            logger.warning("Could not acquire advisory lock - another sync is running")
            
        return self._lock_acquired

    def release_lock(self):
        """Release the advisory lock if held."""
        if self._lock_acquired and self._pg_conn and not self._pg_conn.closed:
            with self._pg_conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(%s)", (ADVISORY_LOCK_ID,))
            self._lock_acquired = False
            logger.info("Advisory lock released")

    # -------------------------------------------------------------------------
    # Sync Run Logging (crm_sync_runs table)
    # -------------------------------------------------------------------------

    def create_sync_run(
        self,
        sync_name: str,
        source_system: str = "close",
        target_table: Optional[str] = None,
    ) -> UUID:
        """Create a new sync run record with status 'running'."""
        result = self.client.table("crm_sync_runs").insert({
            "sync_name": sync_name,
            "source_system": source_system,
            "target_table": target_table,
            "status": "running",
            "started_at": datetime.now(timezone.utc).isoformat(),
        }).execute()
        
        run_id = result.data[0]["id"]
        logger.info(f"Created sync run {run_id} for {sync_name}")
        return UUID(run_id)

    def update_sync_run(
        self,
        run_id: UUID,
        status: str,
        fetched_count: int = 0,
        inserted_count: int = 0,
        updated_count: int = 0,
        skipped_count: int = 0,
        error_count: int = 0,
        error_details: Optional[list] = None,
        cursor_value: Optional[str] = None,
        metadata: Optional[dict] = None,
    ):
        """Update a sync run record with final status and counts."""
        update_data = {
            "status": status,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "fetched_count": fetched_count,
            "inserted_count": inserted_count,
            "updated_count": updated_count,
            "skipped_count": skipped_count,
            "error_count": error_count,
        }
        
        if error_details:
            update_data["error_details"] = error_details
        if cursor_value:
            update_data["cursor_value"] = cursor_value
        if metadata:
            update_data["metadata"] = metadata

        self.client.table("crm_sync_runs").update(update_data).eq("id", str(run_id)).execute()
        logger.info(f"Updated sync run {run_id}: status={status}, fetched={fetched_count}, inserted={inserted_count}, updated={updated_count}")

    def log_skipped_run(self, sync_name: str, reason: str = "lock_not_acquired"):
        """Log a skipped run when lock acquisition fails."""
        self.client.table("crm_sync_runs").insert({
            "sync_name": sync_name,
            "source_system": "close",
            "status": "skipped",
            "started_at": datetime.now(timezone.utc).isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "metadata": {"skip_reason": reason},
        }).execute()
        logger.info(f"Logged skipped run for {sync_name}: {reason}")

    # -------------------------------------------------------------------------
    # Watermark Management (crm_sync_state table)
    # -------------------------------------------------------------------------

    def get_watermark(self, sync_name: str) -> Optional[datetime]:
        """Get the last successful sync timestamp for a sync."""
        result = self.client.table("crm_sync_state").select("last_successful_sync_at").eq("sync_name", sync_name).execute()
        
        if not result.data:
            return None
            
        timestamp_str = result.data[0].get("last_successful_sync_at")
        if not timestamp_str:
            return None
            
        return datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))

    def update_watermark(self, sync_name: str, timestamp: datetime):
        """
        Update the watermark for a sync (only call on successful completion).
        
        Uses upsert to handle first-time creation.
        """
        self.client.table("crm_sync_state").upsert({
            "sync_name": sync_name,
            "last_successful_sync_at": timestamp.isoformat(),
            "last_status": "completed",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }, on_conflict="sync_name").execute()
        
        logger.info(f"Updated watermark for {sync_name} to {timestamp.isoformat()}")

    # -------------------------------------------------------------------------
    # Data Upserts
    # -------------------------------------------------------------------------

    def upsert_leads(self, leads: list[dict]) -> tuple[int, int]:
        """
        Upsert leads to the leads table.
        
        Returns (inserted_count, updated_count).
        Note: Supabase upsert doesn't distinguish insert vs update,
        so we return total as updated_count for simplicity.
        """
        if not leads:
            return 0, 0
            
        result = self.client.table("leads").upsert(
            leads,
            on_conflict="close_lead_id",
        ).execute()
        
        count = len(result.data) if result.data else 0
        return 0, count

    def upsert_custom_activities(self, activities: list[dict]) -> tuple[int, int]:
        """Upsert custom activities."""
        if not activities:
            return 0, 0
            
        result = self.client.table("custom_activities").upsert(
            activities,
            on_conflict="custom_activity_id",
        ).execute()
        
        count = len(result.data) if result.data else 0
        return 0, count

    def upsert_partner_referrals(self, referrals: list[dict]) -> tuple[int, int]:
        """Upsert partner referrals."""
        if not referrals:
            return 0, 0
            
        result = self.client.table("partner_referral").upsert(
            referrals,
            on_conflict="custom_activity_uuid",
        ).execute()
        
        count = len(result.data) if result.data else 0
        return 0, count

    def upsert_partner_uploads(self, uploads: list[dict]) -> tuple[int, int]:
        """Upsert partner uploads."""
        if not uploads:
            return 0, 0
            
        result = self.client.table("partner_upload").upsert(
            uploads,
            on_conflict="custom_activity_uuid",
        ).execute()
        
        count = len(result.data) if result.data else 0
        return 0, count

    def upsert_lead_magnets(self, magnets: list[dict]) -> tuple[int, int]:
        """Upsert lead magnets."""
        if not magnets:
            return 0, 0
            
        result = self.client.table("lead_magnet").upsert(
            magnets,
            on_conflict="custom_activity_uuid",
        ).execute()
        
        count = len(result.data) if result.data else 0
        return 0, count

    def get_leads_by_normalized_name(self) -> dict[str, dict]:
        """
        Map normalized company/lead names to lead records.

        Indexes both entity-normalized names (strips Ltd etc.) and simple
        lowercased names so dealsheet company values can match leads.
        """
        by_name: dict[str, dict] = {}

        def lead_filters(query):
            return query.eq("is_deleted", False)

        for lead in self._paginate_rows(
            "leads",
            "id, close_lead_id, lead_name, display_name",
            _LEAD_PAGE_SIZE,
            lead_filters,
        ):
            close_lead_id = (lead.get("close_lead_id") or "").strip()
            if close_lead_id.startswith("dealsheet_"):
                continue

            record = {
                "id": str(lead["id"]),
                "close_lead_id": close_lead_id or None,
            }
            for name_field in ("lead_name", "display_name"):
                raw = lead.get(name_field) or ""
                for normalized in {
                    _normalize_entity_name(raw),
                    normalize_company(raw),
                }:
                    if normalized and normalized not in by_name:
                        by_name[normalized] = record

        logger.info(f"Built lead name lookup with {len(by_name)} normalized names")
        return by_name

    def attach_lead_to_dealsheet_row(self, row: dict, matcher: LeadMatcher) -> bool:
        """Set lead_id and close_lead_id from the row company name. Returns True if matched."""
        lead = matcher.match(row.get("company") or "")
        if lead:
            row["lead_id"] = lead["id"]
            row["close_lead_id"] = lead.get("close_lead_id")
            return True

        row["lead_id"] = None
        row["close_lead_id"] = None
        return False

    def purge_dealsheet_stub_leads(self) -> tuple[int, int]:
        """
        Remove synthetic dealsheet placeholder leads and unlink dealsheet rows.

        Returns (dealsheet_rows_cleared, stub_leads_deleted).
        """
        stub_lead_ids: list[str] = []
        stub_close_ids: list[str] = []

        for lead in self._paginate_rows(
            "leads",
            "id, close_lead_id",
            _LEAD_PAGE_SIZE,
            lambda q: q.like("close_lead_id", "dealsheet_%"),
        ):
            lead_id = str(lead.get("id") or "").strip()
            close_lead_id = (lead.get("close_lead_id") or "").strip()
            if lead_id:
                stub_lead_ids.append(lead_id)
            if close_lead_id:
                stub_close_ids.append(close_lead_id)

        cleared = 0

        for i in range(0, len(stub_lead_ids), 500):
            chunk = stub_lead_ids[i : i + 500]
            self.client.table("dealsheet_sync_v2").update(
                {"lead_id": None, "close_lead_id": None}
            ).in_("lead_id", chunk).execute()
            cleared += len(chunk)

        pending_uuids: list[str] = []
        for ds in self._paginate_rows(
            "dealsheet_sync_v2",
            "dealsheet_uuid, close_lead_id",
            _DEALSHEET_PAGE_SIZE,
        ):
            close_lead_id = (ds.get("close_lead_id") or "").strip()
            if close_lead_id not in stub_close_ids:
                continue
            pending_uuids.append(ds["dealsheet_uuid"])
            if len(pending_uuids) >= 500:
                self.client.table("dealsheet_sync_v2").update(
                    {"lead_id": None, "close_lead_id": None}
                ).in_("dealsheet_uuid", pending_uuids).execute()
                cleared += len(pending_uuids)
                pending_uuids.clear()

        if pending_uuids:
            self.client.table("dealsheet_sync_v2").update(
                {"lead_id": None, "close_lead_id": None}
            ).in_("dealsheet_uuid", pending_uuids).execute()
            cleared += len(pending_uuids)

        deleted = 0
        for i in range(0, len(stub_close_ids), 500):
            chunk = stub_close_ids[i : i + 500]
            self.client.table("leads").delete().in_("close_lead_id", chunk).execute()
            deleted += len(chunk)

        if cleared or deleted:
            logger.info(
                f"Purged dealsheet stubs: cleared {cleared} dealsheet row links, "
                f"deleted {deleted} stub leads"
            )
        return cleared, deleted

    def backfill_dealsheet_lead_ids(self, matcher: LeadMatcher) -> int:
        """Set lead_id on existing dealsheet rows that are still unlinked."""
        updated = 0
        pending: list[dict] = []

        for ds in self._paginate_rows(
            "dealsheet_sync_v2",
            "dealsheet_uuid,company,lead_id",
            _DEALSHEET_PAGE_SIZE,
        ):
            if ds.get("lead_id") or not (ds.get("company") or "").strip():
                continue
            row = {"company": ds["company"]}
            if self.attach_lead_to_dealsheet_row(row, matcher):
                pending.append(
                    {
                        "dealsheet_uuid": ds["dealsheet_uuid"],
                        "lead_id": row["lead_id"],
                        "close_lead_id": row.get("close_lead_id"),
                    }
                )

            if len(pending) >= 500:
                self.client.table("dealsheet_sync_v2").upsert(
                    pending,
                    on_conflict="dealsheet_uuid",
                ).execute()
                updated += len(pending)
                pending.clear()

        if pending:
            self.client.table("dealsheet_sync_v2").upsert(
                pending,
                on_conflict="dealsheet_uuid",
            ).execute()
            updated += len(pending)

        if updated:
            logger.info(f"Backfilled lead_id on {updated} existing dealsheet rows")
        return updated

    def get_dealsheet_rows_by_company(self) -> dict[str, list[dict]]:
        """Group existing dealsheet rows by normalized company name."""
        by_company: dict[str, list[dict]] = {}
        for row in self._paginate_rows(
            "dealsheet_sync_v2",
            "dealsheet_uuid,company,date",
            1000,
        ):
            company = normalize_company(row.get("company") or "")
            if company:
                by_company.setdefault(company, []).append(row)
        return by_company

    def get_dealsheet_uuids(self) -> set[str]:
        """Return all dealsheet_uuid values currently in dealsheet_sync_v2."""
        return {
            row["dealsheet_uuid"]
            for row in self._paginate_rows(
                "dealsheet_sync_v2",
                "dealsheet_uuid",
                1000,
            )
            if row.get("dealsheet_uuid")
        }

    def upsert_dealsheet_staging(self, rows: list[dict]) -> tuple[int, int]:
        """
        Upsert dealsheet rows into dealsheet_sync_v2.

        Rows are matched on dealsheet_uuid. Rows in the batch are marked active.
        Returns (upserted_count, 0).
        """
        if not rows:
            return 0, 0

        for row in rows:
            row["is_deleted"] = False
            row["deleted_at"] = None

        upserted = 0
        batch_size = 500
        for i in range(0, len(rows), batch_size):
            chunk = rows[i : i + batch_size]
            result = self.client.table("dealsheet_sync_v2").upsert(
                chunk,
                on_conflict="dealsheet_uuid",
            ).execute()
            upserted += len(result.data) if result.data else len(chunk)

        return upserted, 0

    def prune_dealsheet_orphans(self, active_uuids: set[str]) -> int:
        """Delete dealsheet rows that are not in the latest sheet sync."""
        if not active_uuids:
            return 0

        orphan_uuids: list[str] = []
        for row in self._paginate_rows(
            "dealsheet_sync_v2",
            "dealsheet_uuid",
            _DEALSHEET_PAGE_SIZE,
        ):
            dealsheet_uuid = row.get("dealsheet_uuid")
            if dealsheet_uuid and dealsheet_uuid not in active_uuids:
                orphan_uuids.append(dealsheet_uuid)

        if not orphan_uuids:
            return 0

        deleted = 0
        batch_size = 500
        for i in range(0, len(orphan_uuids), batch_size):
            chunk = orphan_uuids[i : i + batch_size]
            self.client.table("dealsheet_sync_v2").delete().in_(
                "dealsheet_uuid", chunk
            ).execute()
            deleted += len(chunk)

        logger.info(f"Deleted {deleted} dealsheet rows not present in current sheet")
        return deleted

    # -------------------------------------------------------------------------
    # Partner Lookup
    # -------------------------------------------------------------------------

    def get_all_partners(self) -> list[dict]:
        """Fetch all active partners for fuzzy matching."""
        result = self.client.table("partners").select(
            "uuid, partner_name, is_active"
        ).eq("is_active", True).eq("is_deleted", False).execute()
        return result.data or []

    def _paginate_rows(self, table: str, columns: str, page_size: int, apply_filters=None):
        """Yield all rows from a table using PostgREST range pagination."""
        offset = 0
        while True:
            query = self.client.table(table).select(columns)
            if apply_filters:
                query = apply_filters(query)
            result = query.range(offset, offset + page_size - 1).execute()
            batch = result.data or []
            yield from batch
            if len(batch) < page_size:
                break
            offset += page_size

    def _build_lead_partner_lookups(self) -> tuple[dict, dict, dict]:
        """
        Build lead lookup maps for paid-partner dealsheet linking.

        Returns:
            (lead_id -> partner_id, close_lead_id -> partner_id, normalized_name -> partner_id)
        """
        lead_by_id: dict[str, str] = {}
        lead_by_close_id: dict[str, str] = {}
        lead_by_normalized_name: dict[str, str] = {}

        def lead_filters(query):
            return query.eq("is_deleted", False).not_.is_("partner_id", "null")

        for lead in self._paginate_rows(
            "leads",
            "id, close_lead_id, lead_name, display_name, partner_id",
            _LEAD_PAGE_SIZE,
            lead_filters,
        ):
            partner_id = lead.get("partner_id")
            if not partner_id:
                continue

            lead_id = lead.get("id")
            if lead_id:
                lead_by_id[str(lead_id)] = partner_id

            close_lead_id = (lead.get("close_lead_id") or "").strip()
            if close_lead_id:
                lead_by_close_id[close_lead_id] = partner_id

            for name_field in ("lead_name", "display_name"):
                normalized = _normalize_entity_name(lead.get(name_field) or "")
                if normalized:
                    lead_by_normalized_name[normalized] = partner_id

        return lead_by_id, lead_by_close_id, lead_by_normalized_name

    def get_funded_partner_uuids(self) -> set[str]:
        """
        Get partner UUIDs that qualify as paid partners from dealsheet_sync_v2.

        A dealsheet row counts only when partner_comms_total_amount is present and > 0.
        The row is linked to a partner via any of:
        - dealsheet.lead_id -> leads.partner_id
        - dealsheet.close_lead_id -> leads.close_lead_id -> leads.partner_id
        - dealsheet.partner_introducer matched to partner_name (fuzzy; active partners preferred)
        - dealsheet.company matched to lead lead_name/display_name -> leads.partner_id

        Returns:
            Set of partner UUIDs (as strings) that should have paid_partner = true
        """
        from partner_matcher import PartnerMatcher

        funded_partner_uuids: set[str] = set()
        link_counts = {
            "lead_id": 0,
            "close_lead_id": 0,
            "introducer": 0,
            "company_name": 0,
        }

        all_partners = self.client.table("partners").select(
            "uuid, partner_name, is_active"
        ).eq("is_deleted", False).execute()
        partner_rows = all_partners.data or []
        active_partners = [p for p in partner_rows if p.get("is_active")]
        matcher = PartnerMatcher(active_partners or partner_rows)

        lead_by_id, lead_by_close_id, lead_by_normalized_name = self._build_lead_partner_lookups()

        commission_row_count = 0

        def dealsheet_filters(query):
            return query.eq("is_deleted", False)

        for ds in self._paginate_rows(
            "dealsheet_sync_v2",
            "lead_id, close_lead_id, partner_introducer, company, partner_comms_total_amount",
            _DEALSHEET_PAGE_SIZE,
            dealsheet_filters,
        ):
            if not _has_partner_commission(ds.get("partner_comms_total_amount")):
                continue

            commission_row_count += 1
            row_linked = False

            lead_id = ds.get("lead_id")
            if lead_id:
                partner_id = lead_by_id.get(str(lead_id))
                if partner_id:
                    funded_partner_uuids.add(partner_id)
                    link_counts["lead_id"] += 1
                    row_linked = True

            close_lead_id = (ds.get("close_lead_id") or "").strip()
            if close_lead_id:
                partner_id = lead_by_close_id.get(close_lead_id)
                if partner_id:
                    funded_partner_uuids.add(partner_id)
                    if not row_linked:
                        link_counts["close_lead_id"] += 1
                    row_linked = True

            introducer = ds.get("partner_introducer")
            if introducer and str(introducer).strip():
                partner_id = matcher.match(introducer)
                if partner_id:
                    funded_partner_uuids.add(partner_id)
                    if not row_linked:
                        link_counts["introducer"] += 1
                    row_linked = True

            company = ds.get("company")
            if company and str(company).strip():
                partner_id = lead_by_normalized_name.get(_normalize_entity_name(company))
                if partner_id:
                    funded_partner_uuids.add(partner_id)
                    if not row_linked:
                        link_counts["company_name"] += 1

        logger.info(
            f"Paid partner scan: {commission_row_count} commission dealsheet rows, "
            f"{len(funded_partner_uuids)} partners linked "
            f"(lead_id={link_counts['lead_id']}, close_lead_id={link_counts['close_lead_id']}, "
            f"introducer={link_counts['introducer']}, company_name={link_counts['company_name']})"
        )

        return funded_partner_uuids

    def get_custom_activity_uuid(self, custom_activity_id: str) -> Optional[str]:
        """Look up the internal UUID for a custom activity by its Close ID."""
        result = self.client.table("custom_activities").select("uuid").eq("custom_activity_id", custom_activity_id).execute()
        
        if result.data:
            return result.data[0]["uuid"]
        return None

    # -------------------------------------------------------------------------
    # Partner Management
    # -------------------------------------------------------------------------

    def create_partner_auth_user(self, partner_name: str, email: str) -> Optional[str]:
        """
        Create a Supabase auth user for a partner.
        
        Args:
            partner_name: Partner name (for metadata)
            email: Generated email for the auth user
            
        Returns:
            Auth user UUID if created successfully, None otherwise
        """
        try:
            # Use Supabase Admin API to create auth user
            response = self.client.auth.admin.create_user({
                "email": email,
                "email_confirm": True,  # Skip email verification
                "user_metadata": {
                    "partner_name": partner_name,
                    "created_by": "close_sync",
                }
            })
            
            if response.user:
                logger.info(f"Created auth user for partner '{partner_name}': {response.user.id}")
                return response.user.id
            return None
            
        except Exception as e:
            logger.error(f"Failed to create auth user for '{partner_name}': {e}")
            return None

    def insert_partner(self, partner_data: dict) -> bool:
        """
        Insert a new partner into the partners table.
        
        Args:
            partner_data: Dict with partner_name, user_id, slug, lead_id, is_active
            
        Returns:
            True if inserted successfully, False otherwise
        """
        try:
            self.client.table("partners").insert(partner_data).execute()
            logger.info(f"Inserted partner: {partner_data['partner_name']}")
            return True
        except Exception as e:
            logger.error(f"Failed to insert partner '{partner_data.get('partner_name')}': {e}")
            return False
