"""Best-effort change logging for sync operations.

All log functions catch and log errors — they never raise.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

CHANGED_BY = "close_sync"


def log_partner_change(
    supabase,
    partner_id: str,
    action: str,
    before_data: Optional[dict] = None,
    after_data: Optional[dict] = None,
):
    """Log a partner insert/update/deactivation to partners_change_log."""
    try:
        supabase.client.table("partners_change_log").insert({
            "partner_id": partner_id,
            "action": action,
            "changed_by": CHANGED_BY,
            "before_data": before_data,
            "after_data": after_data,
        }).execute()
    except Exception as e:
        logger.warning(f"partners_change_log insert failed (non-fatal): {e}")


def log_lead_changes(
    supabase,
    logs: list[dict],
):
    """Batch-insert lead change log entries.

    Each log entry dict: {lead_id, partner_id, action, before_data, after_data, raw_payload}
    """
    if not logs:
        return
    try:
        supabase.client.table("leads_change_log").insert(logs).execute()
    except Exception as e:
        logger.warning(f"leads_change_log insert failed (non-fatal): {e}")


def log_custom_activity_changes(
    supabase,
    logs: list[dict],
):
    """Batch-insert custom activity change log entries.

    Each log entry dict: {activity_id, partner_id, action, before_data, after_data, raw_payload}
    """
    if not logs:
        return
    try:
        supabase.client.table("custom_activities_change_log").insert(logs).execute()
    except Exception as e:
        logger.warning(f"custom_activities_change_log insert failed (non-fatal): {e}")


def log_dealsheet_sync_event(
    supabase,
    stats: dict,
):
    """Log a dealsheet sync completion event to deal_sheet_change_log."""
    try:
        supabase.client.table("deal_sheet_change_log").insert({
            "action": "MERGE",
            "changed_by": CHANGED_BY,
            "details": stats,
        }).execute()
    except Exception as e:
        logger.warning(f"deal_sheet_change_log insert failed (non-fatal): {e}")
