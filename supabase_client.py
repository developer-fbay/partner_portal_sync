"""Supabase client with upserts, watermarks, and advisory locking."""

import logging
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import UUID

import psycopg2
from supabase import create_client, Client

from config import Config

logger = logging.getLogger(__name__)

ADVISORY_LOCK_ID = 1  # Global lock ID shared by all sync modes


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

    # -------------------------------------------------------------------------
    # Partner Lookup
    # -------------------------------------------------------------------------

    def get_all_partners(self) -> list[dict]:
        """Fetch all active partners for fuzzy matching."""
        result = self.client.table("partners").select("uuid, partner_name").eq("is_active", True).execute()
        return result.data or []

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
