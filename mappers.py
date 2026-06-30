"""Data mappers for transforming Close API responses to Supabase schema.

This module implements the Merge Policy from the spec:
- Close-owned columns: overwritten on every upsert (including back to NULL)
- Externally-enriched columns: never written, not included in upsert payload
- Computed columns: derived from Close fields, treated as Close-owned
"""

import hashlib
import logging
import re
from datetime import datetime, timedelta
from typing import Any, Optional

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Column Classification
# -----------------------------------------------------------------------------
# These lists define which columns the sync worker owns vs which are managed externally.
# Close-owned and computed columns are included in upserts.
# Enrichment columns are NOT included - they survive untouched through every sync.

CLOSE_OWNED_LEAD_COLUMNS = [
    # Core identifiers
    "close_lead_id",
    "close_contact_id",
    "close_opportunity_id",
    
    # Basic lead info
    "lead_name",
    "contact_name",
    "contact_email",
    "contact_phone",
    "status",
    "status_id",
    "status_label",
    "description",
    "url",
    "html_url",
    "display_name",
    
    # Partner references (resolved by sync)
    "partner_id",
    "secondary_partner_id",
    "paid_partner",
    
    # Lead source tracking
    "lead_source",
    "lead_magnet_source",
    "campaign",
    "ppc_channel",
    "fb_or_fbx_web_inbound",
    "web_inbound_campaign",
    "webpage_form",
    "outreach_tool",
    "sequence_name",
    "partner_introducer",
    
    # Financial info from Close
    "loan_amount",
    "pay_per_lead",
    
    # Company basic info
    "company_registration_number",
    "company_type",
    "company_status",
    "companies_house_url",
    
    # Close timestamps
    "close_created_at",
    "close_updated_at",
    
    # Primary contact info
    "primary_contact_first_name",
    "primary_contact_last_name",
    "primary_contact_title",
    "primary_contact_primary_phone_type",
    "primary_contact_other_phones",
    "primary_contact_primary_email_type",
    "primary_contact_other_emails",
    "primary_contact_primary_url",
    "primary_contact_other_urls",
    "primary_address_summary",
    
    # Address fields
    "address_1_address_1", "address_1_address_2", "address_1_city", "address_1_state", "address_1_zip", "address_1_country",
    "address_2_address_1", "address_2_address_2", "address_2_city", "address_2_state", "address_2_zip", "address_2_country",
    "address_3_address_1", "address_3_address_2", "address_3_city", "address_3_state", "address_3_zip", "address_3_country",
    "address_4_address_1", "address_4_address_2", "address_4_city", "address_4_state", "address_4_zip", "address_4_country",
    "address_5_address_1", "address_5_address_2", "address_5_city", "address_5_state", "address_5_zip", "address_5_country",
    
    # Ownership / assignment
    "lead_owner_id",
    "lead_owner_name",
    "originator_id",
    "originator_name",
    "partner_owner_id",
    "partner_owner_name",
    "created_by",
    "created_by_name",
    "updated_by",
    "updated_by_name",
    
    # Partner tracking
    "partner_type",
    "partner_split_pct",
    "partner_split_type",
    "partner_prospect_or_lender",
    "partner_agreement_signed_date",
    "first_partner_deal_date",
    "first_partner_lead_sent_date",
    "last_partner_deal_date",
    "last_partner_lead_sent_date",
    "sent_to_partner",
    
    # Triage
    "triage_checked",
    "triaged_by",
    "triage_assist",
    
    # Links
    "google_drive_link",
    "supernormal_link",
    
    # Activity metrics (from Close)
    "last_activity_date",
    "last_activity_type",
    "last_activity_user_id",
    "last_activity_user_name",
    "first_communication_date",
    "first_communication_summary",
    "first_communication_type",
    "first_communication_user_id",
    "first_communication_user_name",
    "last_communication_date",
    "last_communication_summary",
    "last_communication_type",
    "last_communication_user_id",
    "last_communication_user_name",
    "times_communicated",
    
    # Call metrics
    "first_call_created", "first_call_disposition", "first_call_note", "first_call_outcome_id", "first_call_user",
    "last_call_created", "last_call_disposition", "last_call_duration", "last_call_note", "last_call_outcome_id", "last_call_user_id", "last_call_user_name",
    "first_incoming_call_date", "last_incoming_call_date",
    "first_outgoing_call_date", "last_outgoing_call_date",
    "first_voicemail_duration", "last_voicemail_duration",
    "num_calls", "num_incoming_calls", "num_outgoing_calls", "num_missed_calls",
    
    # Email metrics
    "first_email", "first_email_attachments", "first_email_bcc", "first_email_cc", "first_email_created",
    "first_email_from", "first_email_opens", "first_email_template", "first_email_to", "first_email_user",
    "first_emailed", "first_emailed_template",
    "last_email_attachments", "last_email_bcc", "last_email_cc", "last_email_date",
    "last_email_from", "last_email_subject", "last_email_to", "last_email_user",
    "first_incoming_email_date", "last_incoming_email_date",
    "first_outgoing_email_date", "last_outgoing_email_date",
    "email_last_opened", "email_status",
    "num_emails", "num_email_addresses", "num_email_attachments", "num_outgoing_emails", "num_received_emails", "num_sent_emails",
    
    # SMS metrics
    "first_sms_created", "first_sms_date", "first_sms_text", "first_sms_user",
    "last_sms_created", "last_sms_date", "last_sms_text", "last_sms_user",
    "first_incoming_sms_date", "last_incoming_sms_date",
    "first_outgoing_sms_date", "last_outgoing_sms_date",
    "first_received_sms_date", "last_received_sms_date",
    "first_sent_sms_date", "last_sent_sms_date",
    "num_sms", "num_received_sms", "num_sent_sms",
    
    # Meeting metrics
    "first_completed_meeting_outcome_id", "last_completed_meeting_outcome_id",
    "num_meetings", "num_canceled_meetings", "num_completed_meetings", "num_declined_meetings",
    "num_declined_by_lead_meetings", "num_declined_by_org_meetings", "num_in_progress_meetings", "num_upcoming_meetings",
    
    # Note metrics
    "first_note_by", "first_note_user", "last_note_by", "last_note_created", "last_note_user",
    "num_notes",
    
    # Task metrics
    "last_complete_task_due_date", "last_complete_task_updated",
    "last_task_creator", "last_task_due",
    "next_task_date", "next_task_due_date", "next_task_text", "next_task_user_id", "next_task_user_name",
    "num_tasks", "num_completed_tasks", "num_incomplete_tasks",
    
    # Opportunity metrics
    "num_opportunities", "num_active_opportunities", "num_lost_opportunities", "num_won_opportunities",
    "num_annual_opportunities", "num_monthly_opportunities", "num_one_time_opportunities",
    "primary_opportunity_confidence", "primary_opportunity_created", "primary_opportunity_date_won",
    "primary_opportunity_period", "primary_opportunity_pipeline_id", "primary_opportunity_pipeline_name",
    "primary_opportunity_status", "primary_opportunity_status_label", "primary_opportunity_status_type",
    "primary_opportunity_updated", "primary_opportunity_user_id", "primary_opportunity_user_name",
    "primary_opportunity_value", "primary_opportunity_value_summary",
    "first_opportunity_status_change_new_status", "first_opportunity_status_change_old_status",
    "last_opportunity_status_change_date",
    
    # Opportunity value summaries
    "active_opportunity_value_summary", "lost_opportunity_value_summary", "won_opportunity_value_summary", "total_opportunity_value_summary",
    
    # Opportunity value aggregates (avg, min, max, total for annual/monthly/one-time/annualized)
    "avg_annual_active_opportunity_value", "avg_annual_lost_opportunity_value", "avg_annual_opportunity_value", "avg_annual_won_opportunity_value",
    "avg_annualized_active_opportunity_value", "avg_annualized_lost_opportunity_value", "avg_annualized_opportunity_value", "avg_annualized_won_opportunity_value",
    "avg_monthly_active_opportunity_value", "avg_monthly_lost_opportunity_value", "avg_monthly_opportunity_value", "avg_monthly_won_opportunity_value",
    "avg_one_time_active_opportunity_value", "avg_one_time_lost_opportunity_value", "avg_one_time_opportunity_value", "avg_one_time_won_opportunity_value",
    "max_annual_active_opportunity_value", "max_annual_lost_opportunity_value", "max_annual_opportunity_value", "max_annual_won_opportunity_value",
    "max_annualized_active_opportunity_value", "max_annualized_lost_opportunity_value", "max_annualized_opportunity_value", "max_annualized_won_opportunity_value",
    "max_monthly_active_opportunity_value", "max_monthly_lost_opportunity_value", "max_monthly_opportunity_value", "max_monthly_won_opportunity_value",
    "max_one_time_active_opportunity_value", "max_one_time_lost_opportunity_value", "max_one_time_opportunity_value", "max_one_time_won_opportunity_value",
    "min_annual_active_opportunity_value", "min_annual_lost_opportunity_value", "min_annual_opportunity_value", "min_annual_won_opportunity_value",
    "min_annualized_active_opportunity_value", "min_annualized_lost_opportunity_value", "min_annualized_opportunity_value", "min_annualized_won_opportunity_value",
    "min_monthly_active_opportunity_value", "min_monthly_lost_opportunity_value", "min_monthly_opportunity_value", "min_monthly_won_opportunity_value",
    "min_one_time_active_opportunity_value", "min_one_time_lost_opportunity_value", "min_one_time_opportunity_value", "min_one_time_won_opportunity_value",
    "total_annual_active_opportunity_value", "total_annual_lost_opportunity_value", "total_annual_opportunity_value", "total_annual_won_opportunity_value",
    "total_annualized_active_opportunity_value", "total_annualized_lost_opportunity_value", "total_annualized_opportunity_value", "total_annualized_won_opportunity_value",
    "total_monthly_active_opportunity_value", "total_monthly_lost_opportunity_value", "total_monthly_opportunity_value", "total_monthly_won_opportunity_value",
    "total_one_time_active_opportunity_value", "total_one_time_lost_opportunity_value", "total_one_time_opportunity_value", "total_one_time_won_opportunity_value",
    
    # Contact/lead counts
    "num_contacts", "num_contact_urls", "num_addresses", "num_phone_numbers", "num_urls", "num_activities",
    
    # Custom/legacy fields from Close
    "external_uuid",
    "date",
    "disco_date",
    "mob",
    "timestamp",
    "dupe_test",
    "first_source",
    "smart_view_tag",
    "campaign_old",
    "fb_or_fbx",
    "fbx_principal_id",
    "fbx_principal_name",
    "cfa_id",
    "cfa_name",
    "analyst_or_account_manager_id",
    "analyst_or_account_manager_name",
    "accountant",
    "accounting_firm",
    "company_website_lead_crm",
    "further_lead_info",
    "gclid",
    "if_contract_end_date",
    "in_funnel_hot_or_warm",
    "lc_deb_end_date",
    "lc_deb_start_date",
    "mbali_measure",
    "quotezone_gt_6m",
    "r_and_d_hb",
    "sdlt_hb",
    "webpage_or_form",
    "card_rev",
    
    # Raw payload for debugging
    "raw_payload",
]

# Enrichment columns - NEVER written by the sync worker
ENRICHMENT_COLUMNS = [
    "sic_code",
    "years_of_trading",
    "lender",
    "net_assets",
    "profitability",
    "business_model",
    "use_of_funds",
    "turnover",
    "lead_tier",
    "b2b_or_b2c",
    "in_funnel",
    "in_funnel_hot_warm",
    "enrichment_status",
    "industry",
    "sector",
    "incorporation_date",
    "year_end",
    "fixed_assets",
    "profit",
    "net_worth",
    "delphi_score",
    "next_account_due_by",
    "next_account_made_up_to",
    "next_statement_date",
    "next_statement_due_by",
    "debenture_date",
    "debenture_holder",
    "investor_names",
    "latest_funding_date",
    "latest_funding_round",
    "total_funding_raised",
    "product_type",
    "products_taken",
    "payment_processor",
    "fx",
    "eft",
    "vehicle_leasing",
    "discovery_date",
    "engagement_signed_date",
    "reached_dme_date",
    "director_address",
    "director_phone",
    "director_email",
]


# -----------------------------------------------------------------------------
# Close API Field Mapping
# -----------------------------------------------------------------------------

# Mapping from Close API field names to our database column names
CLOSE_TO_DB_FIELD_MAP = {
    "id": "close_lead_id",
    "name": "lead_name",
    "display_name": "display_name",
    "status_id": "status_id",
    "status_label": "status_label",
    "description": "description",
    "url": "url",
    "html_url": "html_url",
    "date_created": "close_created_at",
    "date_updated": "close_updated_at",
    # Custom fields use custom.cf_XXX format - handled dynamically
}

CLOSE_OWNED_LEAD_COLUMN_SET = frozenset(CLOSE_OWNED_LEAD_COLUMNS)
ENRICHMENT_COLUMN_SET = frozenset(ENRICHMENT_COLUMNS)

# Custom label slug overrides (CSV export header → DB column)
_CUSTOM_LABEL_SLUG_OVERRIDES = {
    "in_funnel": "in_funnel",
    "in_funnel_hot_warm": "in_funnel_hot_or_warm",
    "webpage_form": "webpage_or_form",
    "partner_split": "partner_split_pct",
    "partner_prospect_or_lender": "partner_prospect_or_lender",
    "quotezone_6m": "quotezone_gt_6m",
    "r_d_hb": "r_and_d_hb",
    "fb_or_fbx_web_inbound": "fb_or_fbx_web_inbound",
    "fb_fbx": "fb_or_fbx",
    "director_email": "director_email",
    "company_registration_number": "company_registration_number",
    "companies_house_url": "companies_house_url",
    "company_website_lead_crm": "company_website_lead_crm",
    "further_lead_info": "further_lead_info",
    "sic_code": "sic_code",
}

# Close custom user-reference labels → (id_column, name_column)
_CUSTOM_USER_REF_LABELS = {
    "Lead Owner": ("lead_owner_id", "lead_owner_name"),
    "Originator": ("originator_id", "originator_name"),
    "Partner Owner": ("partner_owner_id", "partner_owner_name"),
    "Analyst/Account Manager": ("analyst_or_account_manager_id", "analyst_or_account_manager_name"),
    "CFA": ("cfa_id", "cfa_name"),
    "FBX Principal": ("fbx_principal_id", "fbx_principal_name"),
}

# Top-level Close keys that differ from our DB column names.
CLOSE_SCALAR_FIELD_ALIASES = dict(CLOSE_TO_DB_FIELD_MAP)

# Nested objects handled separately — not copied verbatim in auto-mapping.
_SKIP_LEAD_AUTO_MAP_KEYS = frozenset({
    "contacts",
    "addresses",
    "opportunities",
    "tasks",
    "custom",
    "integration_links",
    "organization_id",
    "models",
})

# Close user references → (id_column, name_column)
_USER_REFERENCE_FIELDS = {
    "created_by": ("created_by", "created_by_name"),
    "updated_by": ("updated_by", "updated_by_name"),
    "lead_owner": ("lead_owner_id", "lead_owner_name"),
    "originator": ("originator_id", "originator_name"),
    "partner_owner": ("partner_owner_id", "partner_owner_name"),
    "triaged_by": ("triaged_by", None),
}

_DATETIME_COLUMN_SUFFIXES = ("_at", "_date", "_created", "_due", "_emailed", "_updated")
_DATETIME_COLUMNS = frozenset(
    column
    for column in CLOSE_OWNED_LEAD_COLUMNS
    if any(column.endswith(suffix) for suffix in _DATETIME_COLUMN_SUFFIXES)
    or column in {
        "first_email",
        "first_emailed",
        "first_call_created",
        "last_call_created",
        "first_sms_created",
        "last_sms_created",
        "email_last_opened",
    }
)

# Postgres `date` columns populated from Close enrichment custom fields
_ENRICHMENT_DATE_COLUMNS = frozenset(
    column
    for column in ENRICHMENT_COLUMNS
    if column.endswith("_date") or column in {"year_end", "next_account_due_by", "next_account_made_up_to", "next_statement_due_by"}
)

_INT_COLUMNS = frozenset(
    column
    for column in CLOSE_OWNED_LEAD_COLUMNS
    if column.startswith("num_") or column in {"years_of_trading", "year"}
)

_NUMERIC_COLUMNS = frozenset(
    column
    for column in CLOSE_OWNED_LEAD_COLUMNS
    if any(token in column for token in ("value", "amount", "rev", "fee", "assets", "profit"))
    or column in {"loan_amount", "pay_per_lead", "card_rev", "partner_split_pct"}
)

# Enrichment columns stored as numeric in Postgres (not in CLOSE_OWNED_LEAD_COLUMNS)
_ENRICHMENT_NUMERIC_COLUMNS = frozenset(
    {
        "profit",
        "net_worth",
        "fixed_assets",
        "total_funding_raised",
        "turnover",
        "net_assets",
    }
)


def _safe_get(data: Any, *keys, default=None):
    """Safely traverse nested dict keys or list indices."""
    result = data
    for key in keys:
        if isinstance(result, dict):
            result = result.get(key, default)
        elif isinstance(result, list) and isinstance(key, int):
            if 0 <= key < len(result):
                result = result[key]
            else:
                return default
        else:
            return default
    return result if result is not None else default


def _normalize_custom_scalar(value: Any) -> Any:
    """Flatten list custom values and stringify for text columns."""
    if isinstance(value, list):
        if not value:
            return None
        if len(value) == 1:
            return _normalize_custom_scalar(value[0])
        return ", ".join(str(item) for item in value if item is not None)
    return value


def _parse_yes_no(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"yes", "true", "1"}:
        return True
    if text in {"no", "false", "0"}:
        return False
    return None


def _parse_datetime(value: Optional[str]) -> Optional[str]:
    """Parse ISO datetime string, return None if invalid."""
    if not value:
        return None
    if not isinstance(value, str):
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt.isoformat()
    except (ValueError, TypeError):
        return None


def _parse_date(value: Optional[str]) -> Optional[str]:
    """Parse a date string to ISO YYYY-MM-DD (handles UK DD/MM/YYYY from Close exports)."""
    if not value:
        return None
    if not isinstance(value, str):
        return None

    value = value.strip()
    if not value:
        return None

    iso_dt = _parse_datetime(value)
    if iso_dt:
        return iso_dt[:10]

    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            continue

    return None


def _extract_custom_field(lead: dict, field_id: str) -> Any:
    """Extract a custom field value from Close lead data."""
    custom = lead.get("custom", {})
    return custom.get(field_id)


def _parse_numeric(value: Any) -> Optional[float]:
    """Parse a value as numeric, return None if invalid."""
    if value is None:
        return None
    try:
        if isinstance(value, list):
            value = value[0] if value else None
            if value is None:
                return None
        if isinstance(value, str):
            text = value.replace(",", "").replace("£", "").replace("$", "").strip()
            if not text or text.upper() in {"N/A", "NA", "TBC", "-", "NONE", "NULL"}:
                return None
            if text.startswith("(") and text.endswith(")"):
                text = f"-{text[1:-1]}"
            if text.upper().endswith("K"):
                return float(text[:-1]) * 1000
            value = text
        return float(value)
    except (ValueError, TypeError, IndexError):
        return None


def _parse_int(value: Any) -> Optional[int]:
    """Parse a value as integer, return None if invalid."""
    if value is None:
        return None
    try:
        # Handle list values (take first element)
        if isinstance(value, list):
            value = value[0] if value else None
            if value is None:
                return None
        if isinstance(value, str):
            value = value.replace(",", "").strip()
            if value == "":
                return None
        return int(float(value))
    except (ValueError, TypeError, IndexError):
        return None


def _extract_primary_contact(lead: dict) -> dict:
    """Extract primary contact info from lead contacts."""
    contacts = lead.get("contacts", [])
    if not contacts:
        return {}
    
    primary = contacts[0]
    
    phones = primary.get("phones", [])
    emails = primary.get("emails", [])
    urls = primary.get("urls", [])
    
    first_name = primary.get("first_name")
    last_name = primary.get("last_name")
    if not first_name and not last_name and primary.get("name"):
        first_name, last_name = _split_contact_name(primary.get("name"))
    
    return {
        "contact_name": primary.get("name"),
        "contact_email": emails[0].get("email") if emails else None,
        "contact_phone": phones[0].get("phone") if phones else None,
        "primary_contact_first_name": first_name,
        "primary_contact_last_name": last_name,
        "primary_contact_title": primary.get("title"),
        "primary_contact_primary_phone_type": phones[0].get("type") if phones else None,
        "primary_contact_other_phones": str([p.get("phone") for p in phones[1:]]) if len(phones) > 1 else None,
        "primary_contact_primary_email_type": emails[0].get("type") if emails else None,
        "primary_contact_other_emails": str([e.get("email") for e in emails[1:]]) if len(emails) > 1 else None,
        "primary_contact_primary_url": urls[0].get("url") if urls else None,
        "primary_contact_other_urls": str([u.get("url") for u in urls[1:]]) if len(urls) > 1 else None,
    }


def _extract_addresses(lead: dict) -> dict:
    """Extract address fields from lead."""
    addresses = lead.get("addresses", [])
    result = {}
    
    for i, addr in enumerate(addresses[:5], 1):
        prefix = f"address_{i}_"
        result[f"{prefix}address_1"] = addr.get("address_1")
        result[f"{prefix}address_2"] = addr.get("address_2")
        result[f"{prefix}city"] = addr.get("city")
        result[f"{prefix}state"] = addr.get("state")
        result[f"{prefix}zip"] = addr.get("zipcode")
        result[f"{prefix}country"] = addr.get("country")
    
    return result


# Lead custom field IDs → (db_column, parser)
LEAD_CUSTOM_FIELD_MAPPINGS = {
    "lcf_pHVheIfAnOIBdoGLVhHxQmav8hxGOb2i6Ar8h2tuV77": ("lead_source", None),
    "lcf_fSb5j0xDXyJiKLwdHPYjvRCdbkpUJJpn9krReNKNOyy": ("partner_introducer", None),
    "lcf_yDWySRCcDFhvCrK0UzCI5slU1XSYCP4ni4j6ZSODBzD": ("company_registration_number", None),
    "lcf_Scsqcdb6i2SetF7ZUYLJ1k3sKFBCTzmgB3z1U9ebLN0": ("lead_owner_id", None),
    "lcf_8s1eNJGYT2cmv722G5LRltCAlluT5fYOLfjLDKtTvcK": ("partner_prospect_or_lender", _normalize_custom_scalar),
    "cf_cbkJ31wiw7qsL5IjCGMKyEEneuzd9fgXSz5B0vOi2CA": ("loan_amount", _parse_numeric),
    "cf_1BaclVASPoUDWFKNh2WwoU74O4RZhIZfInJA9WvipZ9": ("company_registration_number", None),
    "cf_KlLz6gUfydy56wh6Lm0Qkc1032pIWle1auZ6fQ6p6Uw": ("companies_house_url", None),
    "cf_rjb2fDFC55AdRComep4ycZgDtg5Xcx0fYPIM7aE5n8N": ("years_of_trading", _parse_int),
    "cf_83OWakxD8TRT2UjzqbRMW3t1m9IRydJQC7hais5gXls": ("paid_partner", _parse_yes_no),
    "cf_F2lNv5XrVUBxKGPmXWxqPna9VZqDaqkDREe4rLi4YxC": ("partner_split_pct", _normalize_custom_scalar),
    "cf_GCBQx6Sht3pwfWjPxdcCOJnZVgHnXnAiPwZFIlf2diD": ("partner_split_type", None),
    "cf_ILleH7M40c2ps0jXxhyTgMdOj8eKLqdDQK2QDeRdfFB": ("originator_id", None),
    "cf_VW2Vz2O4XrH3O3VcAoH0CXZRVPnRo8CrT27MDO9CXf7": ("partner_owner_id", _normalize_custom_scalar),
    "cf_hi0bhzuTnyvY6HnZlhPZwMQAOtGz01XZhnlmfrIM7HJ": ("fb_or_fbx", None),
    "cf_qvzmy7ALxb2Szbt4Jn3lSCBlY5OAjmm8io5A9v03oB6": ("smart_view_tag", None),
}

# Human-readable Close custom labels (custom dict keys) → (db_column, parser)
LEAD_CUSTOM_LABEL_MAPPINGS = {
    "Lead Source": ("lead_source", None),
    "Partner Introducer": ("partner_introducer", None),
    "Company Registration number": ("company_registration_number", None),
    "Lead Owner": ("lead_owner_id", None),
    "Originator": ("originator_id", None),
    "Paid partner": ("paid_partner", _parse_yes_no),
    "Partner Owner": ("partner_owner_id", _normalize_custom_scalar),
    "Partner split %": ("partner_split_pct", _normalize_custom_scalar),
    "Partner split type": ("partner_split_type", None),
    "Partner, Prospect or Lender?": ("partner_prospect_or_lender", _normalize_custom_scalar),
    "Smart View Tag": ("smart_view_tag", None),
    "FB/FBX": ("fb_or_fbx", None),
    "Campaign": ("campaign", None),
    "Campaign (OLD)": ("campaign_old", None),
    "Turnover": ("turnover", _parse_numeric),
    "SIC Code": ("sic_code", None),
    "Industry": ("industry", None),
    "Sector": ("sector", None),
    "Accountant": ("accountant", None),
    "Accounting Firm": ("accounting_firm", None),
    "Company Website (Lead CRM)": ("company_website_lead_crm", None),
    "Companies House URL": ("companies_house_url", None),
    "Company Status": ("company_status", None),
    "Company Type": ("company_type", None),
    "B2B or B2C": ("b2b_or_b2c", None),
    "Business Model": ("business_model", None),
    "Use of Funds": ("use_of_funds", None),
    "Lender": ("lender", None),
    "Net Assets": ("net_assets", _parse_numeric),
    "Profitability": ("profitability", None),
    "Years of Trading": ("years_of_trading", _parse_int),
    "In Funnel": ("in_funnel", None),
    "In Funnel Hot/Warm": ("in_funnel_hot_or_warm", None),
    "Enrichment status": ("enrichment_status", None),
    "GCLID": ("gclid", None),
    "Card Rev": ("card_rev", _parse_numeric),
    "Date": ("date", None),
    "Disco Date": ("disco_date", None),
    "Discovery Date": ("discovery_date", None),
    "Dupe test": ("dupe_test", None),
    "First Source": ("first_source", None),
    "Further Lead Info": ("further_lead_info", None),
    "IF Contract End Date": ("if_contract_end_date", None),
    "LC Deb End Date": ("lc_deb_end_date", None),
    "LC Deb Start Date": ("lc_deb_start_date", None),
    "Mbali measure": ("mbali_measure", None),
    "Quotezone >6m": ("quotezone_gt_6m", None),
    "R&D HB": ("r_and_d_hb", None),
    "SDLT HB": ("sdlt_hb", None),
    "Webpage/Form": ("webpage_or_form", None),
    "FB or FBX Web Inbound": ("fb_or_fbx_web_inbound", None),
    "Web Inbound Campaign": ("web_inbound_campaign", None),
    "Outreach Tool": ("outreach_tool", None),
    "Sequence Name": ("sequence_name", None),
    "Split Lender %": ("split_lender_pct", None),
    "Split Success %": ("split_success_pct", None),
    "Partner Type": ("partner_type", None),
    "Sent to Partner": ("sent_to_partner", None),
    "Google Drive Link": ("google_drive_link", None),
    "Supernormal Link": ("supernormal_link", None),
    "Triage Checked": ("triage_checked", None),
    "Triage Assist": ("triage_assist", None),
    "External UUID": ("external_uuid", None),
    "MOB": ("mob", None),
    "Timestamp": ("timestamp", None),
    "Pay Per Lead": ("pay_per_lead", _parse_numeric),
    "Loan Amount": ("loan_amount", _parse_numeric),
}


def _custom_label_to_db_column(label: str) -> Optional[str]:
    """Map a Close custom field label to a DB column (owned or enrichment)."""
    if label in LEAD_CUSTOM_LABEL_MAPPINGS:
        return LEAD_CUSTOM_LABEL_MAPPINGS[label][0]
    slug = re.sub(
        r"[^a-z0-9]+",
        "_",
        label.lower().replace("/", "_or_").replace("?", ""),
    ).strip("_")
    slug = _CUSTOM_LABEL_SLUG_OVERRIDES.get(slug, slug)
    if slug in CLOSE_OWNED_LEAD_COLUMN_SET or slug in ENRICHMENT_COLUMN_SET:
        return slug
    return None


def _split_contact_name(full_name: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    if not full_name:
        return None, None
    parts = full_name.strip().split(None, 1)
    if len(parts) == 1:
        return parts[0], None
    return parts[0], parts[1]


def _resolve_user_id(value: Any) -> Optional[str]:
    """Extract a Close user id from a custom user-reference value."""
    normalized = _normalize_custom_scalar(value)
    if isinstance(normalized, str) and normalized.startswith("user_"):
        return normalized
    return None


def _coerce_lead_scalar(db_column: str, value: Any) -> Any:
    """Coerce a Close scalar/smart-field value to the appropriate DB type."""
    if value is None or value == "":
        return None
    if db_column == "paid_partner":
        return _parse_yes_no(value)
    if isinstance(value, (dict, list)):
        if db_column in _INT_COLUMNS or db_column in _NUMERIC_COLUMNS:
            return None
        return _normalize_custom_scalar(value)

    if db_column in _DATETIME_COLUMNS:
        return _parse_datetime(value) if isinstance(value, str) else value
    if db_column in _ENRICHMENT_DATE_COLUMNS:
        return _parse_date(value) if isinstance(value, str) else value
    if db_column in _INT_COLUMNS:
        return _parse_int(value)
    if db_column in _NUMERIC_COLUMNS or db_column in _ENRICHMENT_NUMERIC_COLUMNS:
        return _parse_numeric(value)
    return value


def _set_if_empty(mapped: dict, db_column: str, value: Any) -> None:
    if db_column not in CLOSE_OWNED_LEAD_COLUMN_SET:
        return
    if mapped.get(db_column) is not None:
        return
    mapped[db_column] = _coerce_lead_scalar(db_column, value)


def _assign_close_owned(mapped: dict, db_column: str, value: Any) -> None:
    """Assign a Close-owned column, including explicit nulls (merge policy)."""
    if db_column not in CLOSE_OWNED_LEAD_COLUMN_SET:
        return
    mapped[db_column] = _coerce_lead_scalar(db_column, value)


def _map_custom_fields(
    lead: dict,
    mapped: dict,
    user_names: Optional[dict[str, str]] = None,
) -> None:
    custom = lead.get("custom") or {}

    for close_field, (db_column, parser) in LEAD_CUSTOM_FIELD_MAPPINGS.items():
        value = custom.get(close_field)
        if value is None and close_field in lead:
            value = lead.get(close_field)
        if value is None and f"custom.{close_field}" in lead:
            value = lead.get(f"custom.{close_field}")
        if value is None:
            continue
        parsed = parser(value) if parser else value
        _assign_mapped_column(mapped, db_column, parsed)

    for label, (db_column, parser) in LEAD_CUSTOM_LABEL_MAPPINGS.items():
        value = custom.get(label)
        if value is None:
            continue
        if mapped.get(db_column) is not None:
            continue
        parsed = parser(value) if parser else value
        _assign_mapped_column(mapped, db_column, parsed)

    for label, (id_column, name_column) in _CUSTOM_USER_REF_LABELS.items():
        user_id = _resolve_user_id(custom.get(label))
        if not user_id:
            continue
        _set_if_empty(mapped, id_column, user_id)
        if name_column and user_names:
            _set_if_empty(mapped, name_column, user_names.get(user_id))

    mapped_labels = set(LEAD_CUSTOM_LABEL_MAPPINGS) | set(_CUSTOM_USER_REF_LABELS)
    for label, value in custom.items():
        if label.startswith(("cf_", "lcf_")):
            continue
        if label in mapped_labels:
            continue
        db_column = _custom_label_to_db_column(label)
        if not db_column or mapped.get(db_column) is not None:
            continue
        _assign_mapped_column(mapped, db_column, value)


def _assign_mapped_column(mapped: dict, db_column: str, value: Any) -> None:
    """Write a mapped value to Close-owned or Close-sourced enrichment columns."""
    if db_column in CLOSE_OWNED_LEAD_COLUMN_SET:
        mapped[db_column] = _coerce_lead_scalar(db_column, value)
    elif db_column in ENRICHMENT_COLUMN_SET and value is not None:
        mapped[db_column] = _coerce_lead_scalar(db_column, value)


def _map_auto_fields(lead: dict, mapped: dict) -> None:
    """Map top-level Close fields and smart fields that match DB column names."""
    for key, value in lead.items():
        if key in _SKIP_LEAD_AUTO_MAP_KEYS or key.startswith("custom."):
            continue

        db_column = CLOSE_SCALAR_FIELD_ALIASES.get(key, key)
        _assign_close_owned(mapped, db_column, value)


def _map_user_references(
    lead: dict,
    mapped: dict,
    user_names: Optional[dict[str, str]] = None,
) -> None:
    for close_key, (id_column, name_column) in _USER_REFERENCE_FIELDS.items():
        value = lead.get(close_key)
        if value is None:
            continue

        if isinstance(value, dict):
            _set_if_empty(mapped, id_column, value.get("id"))
            if name_column:
                _set_if_empty(
                    mapped,
                    name_column,
                    value.get("name") or value.get("display_name"),
                )
        else:
            _set_if_empty(mapped, id_column, value)
            if name_column and user_names and isinstance(value, str):
                _set_if_empty(mapped, name_column, user_names.get(value))


def _map_primary_opportunity(lead: dict, mapped: dict) -> None:
    opportunities = lead.get("opportunities") or []
    if not opportunities:
        return

    primary = next(
        (opp for opp in opportunities if opp.get("status_type") == "active"),
        opportunities[0],
    )

    opportunity_field_map = {
        "id": "close_opportunity_id",
        "status_id": "primary_opportunity_status",
        "status_label": "primary_opportunity_status_label",
        "status_type": "primary_opportunity_status_type",
        "confidence": "primary_opportunity_confidence",
        "date_created": "primary_opportunity_created",
        "date_won": "primary_opportunity_date_won",
        "value_period": "primary_opportunity_period",
        "pipeline_id": "primary_opportunity_pipeline_id",
        "pipeline_name": "primary_opportunity_pipeline_name",
        "value": "primary_opportunity_value",
        "value_formatted": "primary_opportunity_value_summary",
        "date_updated": "primary_opportunity_updated",
        "user_id": "primary_opportunity_user_id",
        "user_name": "primary_opportunity_user_name",
    }

    for opp_key, db_column in opportunity_field_map.items():
        if opp_key not in primary:
            continue
        value = primary[opp_key]
        if opp_key in {"date_created", "date_won", "date_updated"}:
            value = _parse_datetime(value)
        elif opp_key in {"confidence", "value"}:
            value = _parse_numeric(value)
        _set_if_empty(mapped, db_column, value)


def map_lead(
    lead: dict,
    partner_id: Optional[str] = None,
    secondary_partner_id: Optional[str] = None,
) -> dict:
    """
    Map a Close smart-view search lead to Close-owned database columns only.

    Used by --phase leads. For full export columns use map_lead_full in lead_details.
    """
    mapped = {
        "close_lead_id": lead.get("id"),
        "close_contact_id": _safe_get(lead, "contacts", 0, "id") if lead.get("contacts") else None,
        "lead_name": lead.get("name"),
        "display_name": lead.get("display_name"),
        "status_id": lead.get("status_id"),
        "status_label": lead.get("status_label"),
        "description": lead.get("description"),
        "url": lead.get("url"),
        "html_url": lead.get("html_url"),
        "close_created_at": _parse_datetime(lead.get("date_created")),
        "close_updated_at": _parse_datetime(lead.get("date_updated")),
        "partner_id": partner_id,
        "secondary_partner_id": secondary_partner_id,
        "raw_payload": lead,
    }

    mapped.update(_extract_primary_contact(lead))
    mapped.update(_extract_addresses(lead))

    custom = lead.get("custom", {})
    lead_custom_field_mappings = {
        "lcf_pHVheIfAnOIBdoGLVhHxQmav8hxGOb2i6Ar8h2tuV77": ("lead_source", None),
        "lcf_fSb5j0xDXyJiKLwdHPYjvRCdbkpUJJpn9krReNKNOyy": ("partner_introducer", None),
        "lcf_yDWySRCcDFhvCrK0UzCI5slU1XSYCP4ni4j6ZSODBzD": ("lead_magnet_source", None),
        "cf_cbkJ31wiw7qsL5IjCGMKyEEneuzd9fgXSz5B0vOi2CA": ("loan_amount", _parse_numeric),
        "cf_1BaclVASPoUDWFKNh2WwoU74O4RZhIZfInJA9WvipZ9": ("company_registration_number", None),
        "cf_I4j4JTIiWTyytx3pQQrClCmvoiyj3kEPbHZJZ9Lu03Q": ("sic_code", None),
        "cf_rjb2fDFC55AdRComep4ycZgDtg5Xcx0fYPIM7aE5n8N": ("years_of_trading", _parse_int),
        "cf_KlLz6gUfydy56wh6Lm0Qkc1032pIWle1auZ6fQ6p6Uw": ("companies_house_url", None),
        "cf_uJBUmk6iurAW4YB9ruTtnQWQhSfbftRadN4arHZa6Gl": ("lender", None),
        "cf_1UTRFkeXgUVAnQvZHJJjH4rHkDp70qyLbmzzUFAeVTY": ("net_assets", _parse_numeric),
        "cf_agED7vPcyJttr6nWLWmqyyiN2LL2QYSXFFJbvHocVYX": ("profitability", None),
        "cf_G224hFBn5sde4b7zX3rdejIY0P5dO6zRNyf2Dx1Pfh3": ("turnover", _parse_numeric),
    }

    for close_field, (db_column, parser) in lead_custom_field_mappings.items():
        if close_field in custom:
            value = custom[close_field]
            mapped[db_column] = parser(value) if parser else value

    result = {}
    for key, value in mapped.items():
        if key in CLOSE_OWNED_LEAD_COLUMNS or key == "close_lead_id":
            result[key] = value

    return result


def map_lead_full(
    lead: dict,
    partner_id: Optional[str] = None,
    secondary_partner_id: Optional[str] = None,
    user_names: Optional[dict[str, str]] = None,
) -> dict:
    """
    Map a full Close lead (?_fields=_all) to Close-owned and enrichment columns.

    Used by --phase lead_details only.
    """
    mapped = {
        "close_lead_id": lead.get("id"),
        "close_contact_id": _safe_get(lead, "contacts", 0, "id") if lead.get("contacts") else None,
        "lead_name": lead.get("name"),
        "display_name": lead.get("display_name"),
        "status_id": lead.get("status_id"),
        "status_label": lead.get("status_label"),
        "description": lead.get("description"),
        "url": lead.get("url"),
        "html_url": lead.get("html_url"),
        "close_created_at": _parse_datetime(lead.get("date_created")),
        "close_updated_at": _parse_datetime(lead.get("date_updated")),
        "partner_id": partner_id,
        "secondary_partner_id": secondary_partner_id,
        "raw_payload": lead,
    }

    mapped.update(_extract_primary_contact(lead))
    mapped.update(_extract_addresses(lead))
    _map_custom_fields(lead, mapped, user_names=user_names)
    _map_user_references(lead, mapped, user_names=user_names)
    _map_primary_opportunity(lead, mapped)
    _map_auto_fields(lead, mapped)

    for column in CLOSE_OWNED_LEAD_COLUMNS:
        if column.startswith("num_") and isinstance(mapped.get(column), (int, float)):
            mapped[column] = str(mapped[column])

    if lead.get("status") is not None:
        _set_if_empty(mapped, "status", lead.get("status"))

    if mapped.get("primary_address_summary") is None:
        addresses = lead.get("addresses") or []
        if addresses:
            first = addresses[0]
            parts = [
                first.get("address_1"),
                first.get("city"),
                first.get("zipcode"),
                first.get("country"),
            ]
            summary = ", ".join(part for part in parts if part)
            if summary:
                mapped["primary_address_summary"] = summary

    result = {}
    for key, value in mapped.items():
        if key in CLOSE_OWNED_LEAD_COLUMNS or key == "close_lead_id":
            result[key] = value
        elif key in ENRICHMENT_COLUMN_SET and value is not None:
            result[key] = value

    return result


def map_custom_activity(activity: dict, partner_id: Optional[str] = None) -> dict:
    """
    Map a Close custom activity to the custom_activities table.
    
    Args:
        activity: Raw activity data from Close API
        partner_id: Resolved partner UUID
        
    Returns:
        Dict ready for upsert to custom_activities
    """
    return {
        "custom_activity_id": activity.get("id"),
        "partner_id": partner_id,
        "source_system": "close_crm",
        "custom_activity_type_id": activity.get("custom_activity_type_id"),
        "lead_id": activity.get("lead_id"),
        "updated_at": datetime.utcnow().isoformat(),
    }


def _get_custom_field(custom: dict, field_id: str) -> Any:
    """Read a custom field by Close field id from a normalized activity custom dict."""
    if not custom or not field_id:
        return None
    return custom.get(field_id)


def _format_activity_custom_value(
    value: Any,
    user_names: Optional[dict[str, str]] = None,
) -> Any:
    """Coerce activity custom values (user, contact, choices, lists) to text."""
    if value is None or value == "":
        return None

    if isinstance(value, dict):
        user_id = value.get("id")
        if isinstance(user_id, str) and user_id.startswith("user_"):
            if user_names and user_id in user_names:
                return user_names[user_id]
            return value.get("name") or value.get("display_name") or user_id
        return value.get("name") or value.get("id") or str(value)

    normalized = _normalize_custom_scalar(value)
    if isinstance(normalized, str) and normalized.startswith("user_") and user_names:
        return user_names.get(normalized, normalized)
    if isinstance(normalized, list):
        return ", ".join(str(item) for item in normalized if item is not None)
    return normalized


def _read_activity_custom(
    custom: dict,
    field_id: str,
    user_names: Optional[dict[str, str]] = None,
) -> Any:
    return _format_activity_custom_value(
        _get_custom_field(custom, field_id),
        user_names=user_names,
    )


def map_partner_referral(
    activity: dict,
    custom_activity_uuid: str,
    user_names: Optional[dict[str, str]] = None,
) -> dict:
    """
    Map a partner referral activity to the partner_referral table.

    Handles both activity types:
    - GEN1. Referral Upload (actitype_1CKUCsigQLAPoNmDABmjcj)
    - API - Referral Upload (actitype_0PpighCxVchK68dd8Hknzd)
    """
    custom = activity.get("custom", {}) or {}
    activity_type = activity.get("custom_activity_type_id")

    GEN1_PARTNER_OWNER = "cf_gf3TBdO9xqr3LB9SP6XJMX7u5GbRFgknoIPGo0mVF2B"
    GEN1_PARTNER_CONTACT = "cf_kPxEAcMd27hQCKk4RBXSIwfuav4QyPrpzGcwX04Lvlp"
    GEN1_BROKER = "cf_Xi0eO5V1VWXDHdLNvwcn7oFHSBtdKBuHbA5UYalIrgW"
    GEN1_TYPE = "cf_CqnuZdXRLGdEBfoNojmtNh3GkGSxmFLrykItNz4F7Eq"
    GEN1_COMPANY = "cf_E9gIFnO0ybTo1VgNjmIghOmAPnaLIeuc4gQEueY7LVm"
    GEN1_COMPANY_NUMBER = "cf_Fh7nPBkHbs0PLXRKxgpsNWnxoofflKYdLtzzcqy3pUJ"
    GEN1_CONTACT_NAME = "cf_WmVlnmCidqZGKmed44H5sloPxcUGVqVVwgI4Qg0yF7z"
    GEN1_CONTACT_PHONE = "cf_4kH5ScazObc7Phz5Fuqdsc2MuW9W3Lsmbufhn39fmbK"
    GEN1_CONTACT_EMAIL = "cf_4K6Xv4zg50JQxyRQMRcL2MiolAkszKQqW5XiYoSCzay"
    GEN1_FB_FBX = "cf_Y6OET4Q82lVHDrPblXhJHA2Ojaz7Mvl57kV8sCM9fHP"
    GEN1_NOTES = "cf_TQbqzxpfF6RsGWqxK6z9cQFWpMD4ZssEWK99Q8YjUQh"

    API_PARTNER_OWNER = "cf_uu8Zvx863K7mxRgKRFZh9o4ORAbzTBAYOpzHSgXJGIl"
    API_PARTNER = "cf_0gmxLCFokmWcEfaq5yQbK7AwJAjkI4MZ1rLkLG8wu1c"
    API_TYPE = "cf_xOmyo63cf4EslqGvyScZGnSC7axiT0fBfmIjbxBClya"
    API_NOTES = "cf_PjpQQJQCpdq9mOYKynKuUHp7knWSl8miqehVpd1Sbdn"
    API_ORIGINAL_FORM = "cf_ryELA04EaodpIf5nFTL0AQSfzRmw3EBK8wC0lC6z8YP"

    def has_field(field_id: str) -> bool:
        return _get_custom_field(custom, field_id) is not None

    def read(field_id: str) -> Any:
        return _read_activity_custom(custom, field_id, user_names)

    def first_set(*values: Any) -> Any:
        for value in values:
            if value is not None and value != "":
                return value
        return None

    is_gen1 = activity_type == "actitype_1CKUCsigQLAPoNmDABmjcj"
    is_api = activity_type == "actitype_0PpighCxVchK68dd8Hknzd"

    partner_owner = first_set(
        read(GEN1_PARTNER_OWNER) if is_gen1 or has_field(GEN1_PARTNER_OWNER) else None,
        read(API_PARTNER_OWNER) if is_api or has_field(API_PARTNER_OWNER) else None,
    )
    company_name = first_set(
        read(GEN1_COMPANY) if is_gen1 or has_field(GEN1_COMPANY) else None,
        read(API_PARTNER) if is_api or has_field(API_PARTNER) else None,
    )
    type_of_partner = first_set(
        read(GEN1_TYPE) if is_gen1 or has_field(GEN1_TYPE) else None,
        read(API_TYPE) if is_api or has_field(API_TYPE) else None,
    )
    notes = first_set(
        read(GEN1_NOTES) if is_gen1 or has_field(GEN1_NOTES) else None,
        read(API_NOTES) if is_api or has_field(API_NOTES) else None,
    )
    contact_phone = read(GEN1_CONTACT_PHONE) if (
        is_gen1 or has_field(GEN1_CONTACT_PHONE)
    ) else None
    contact_email = read(GEN1_CONTACT_EMAIL) if (
        is_gen1 or has_field(GEN1_CONTACT_EMAIL)
    ) else None
    contact_name = first_set(
        read(GEN1_CONTACT_NAME) if is_gen1 or has_field(GEN1_CONTACT_NAME) else None,
        read(GEN1_PARTNER_CONTACT) if is_gen1 or has_field(GEN1_PARTNER_CONTACT) else None,
    )

    lead_owner = (
        activity.get("user_name")
        or activity.get("created_by_name")
        or partner_owner
    )

    return {
        "custom_activity_uuid": custom_activity_uuid,
        "custom_activity_id": activity.get("id"),
        "lead_owner": lead_owner,
        "company_name": company_name,
        "contact_number": contact_phone,
        "contact_email_address": contact_email,
        "contact_name": contact_name,
        "additional_notes": read(API_ORIGINAL_FORM) if (
            is_api or has_field(API_ORIGINAL_FORM)
        ) else None,
        "partner_owner": partner_owner,
        "broker_to_send_to": read(GEN1_BROKER) if (
            is_gen1 or has_field(GEN1_BROKER)
        ) else None,
        "type_of_partner": type_of_partner,
        "company_number": read(GEN1_COMPANY_NUMBER) if (
            is_gen1 or has_field(GEN1_COMPANY_NUMBER)
        ) else None,
        "contact_phone_number": contact_phone,
        "fb_fbx": read(GEN1_FB_FBX) if is_gen1 or has_field(GEN1_FB_FBX) else None,
        "notes": notes,
        "updated_at": datetime.utcnow().isoformat(),
    }


def map_partner_upload(
    activity: dict,
    custom_activity_uuid: str,
    user_names: Optional[dict[str, str]] = None,
) -> dict:
    """
    Map a partner upload activity to the partner_upload table.

    GEN2. New Partner Upload (actitype_5rvWuLY9CJ1bPIAYUU8wCS)
    """
    custom = activity.get("custom", {}) or {}

    # GEN2. New Partner Upload — field ids from Close activity type schema
    GEN2_LEAD_OWNER = "cf_BCWS1FSqD1wOxhSCWnGnu8r6qutgSItA6Tvs2N50OzS"
    GEN2_COMPANY_NAME = "cf_LA6Bq4P762ZPSIiu2YGOtk0tnYPegrJHzlZSj8Aycvx"
    GEN2_CONTACT_NUMBER = "cf_87fWr1GfDHvkIrwhSnohzjKNCovOhpFw9P3UhzresFR"
    GEN2_CONTACT_EMAIL = "cf_wWeeTZeE8wSgJL7O9vctDrA3kMq2kWbptxtVgAnqx8a"
    GEN2_CONTACT_NAME = "cf_t5v6hQj4Jz4KzoMZY2dcXBzpXzms8KyvwTq0S5gOopq"
    GEN2_ADDITIONAL_NOTES = "cf_idsVe9yIYNdfY6akz02nzoWtOllyLTYg0GlkQveVtF8"

    lead_owner = _read_activity_custom(custom, GEN2_LEAD_OWNER, user_names)
    if not lead_owner:
        lead_owner = activity.get("user_name") or activity.get("created_by_name")

    return {
        "custom_activity_uuid": custom_activity_uuid,
        "custom_activity_id": activity.get("id"),
        "lead_owner": lead_owner,
        "company_name": _read_activity_custom(custom, GEN2_COMPANY_NAME, user_names),
        "contact_number": _read_activity_custom(custom, GEN2_CONTACT_NUMBER, user_names),
        "contact_email_address": _read_activity_custom(
            custom, GEN2_CONTACT_EMAIL, user_names
        ),
        "contact_name": _read_activity_custom(custom, GEN2_CONTACT_NAME, user_names),
        "additional_notes": _read_activity_custom(
            custom, GEN2_ADDITIONAL_NOTES, user_names
        ),
        "updated_at": datetime.utcnow().isoformat(),
    }


def map_lead_magnet(activity: dict, custom_activity_uuid: str) -> dict:
    """
    Map a LeadMaggy activity to the lead_magnet table.
    
    Handles both activity types (they have different field IDs for same data):
    - LeadMaggy (actitype_7F05YTbEK5kDTySb2WN7de)
    - LeadMaggy - Updated (actitype_7YnfLQNfeZsBTMN3ADcCZf)
    
    Args:
        activity: Raw activity data from Close API
        custom_activity_uuid: UUID from custom_activities table
        
    Returns:
        Dict ready for upsert to lead_magnet
    """
    custom = activity.get("custom", {}) or {}
    
    # Field ID mappings for LeadMaggy (original)
    leadmaggy_fields = {
        "loan_amount": "cf_frNyEGvv5o0qq1WBsCxKQbsUGyuSSWaoAdfqIOAy0pB",
        "lead_source": "cf_MtlwzPvj2UqqmExz45WwLBcAm0OwCIVjETvTPHuGnTA",
        "lead_magnet_source": "cf_Gv6PXGEZfHRo4YO3mrZFVkuhLnWA7so4wnKMHDULfGH",
        "company_reg": "cf_DZ7Stw3Qkf8zvkMZjEUhTSvonKKiFJIvdOGcczJlMsK",
        "sic": "cf_N6uDN5tSEwozqsgb2olrJCNE7CWOtC7rP1sERlZSTBf",
        "years_of_trading": "cf_G2fac4zr9Ygv7X6unh9W7Q3fg3048tWz3N8iexiDUgP",
        "companies_house_url": "cf_TrUNRnxxGduUdO8drgDSRY6hxKPbcPVPEOn4OJcR5MA",
        "lender": "cf_OTqa92boQ4EEUyv9vm5Px2GwBUEazaBY05Lry5Hzer1",
        "net_assets": "cf_ldmzYVAhPbP47FtNV3DOxq8HX9YBqaeFxrWaAOPjfa2",
        "profitability": "cf_xTzPsx4ivu15bABsUqPL8ghyb5cadVMlCy6vatbC1oq",
        "business_model": "cf_iAsr2Fsi54gHukPJclaYpRSFFIlSgkBcBo12GBcd6Gx",
        "use_of_funds": "cf_HPDj4zz43rbq7NpwX9ZixUp2e5JdaVWbuFPZP9BIy1U",
        "turnover": "cf_sSc96mfQ9xuZQblBfUae1qYp0OW12WRDzCC1e8gQSzs",
        "company_type": "cf_Uz7jV1ELt0jx5UlZQOYe49aVbSupwwUWaCD27Eh7Gz1",
        "company_status": "cf_ndVhyS6oIRxeisUZISvJPpyphIKiJjeoX0ozDyzkVSS",
        "external_uuid": "cf_1cXRbzyU2ve2Q4TaLcKgOkqmcoI9o17zecDdIj0OuhU",
        "pay_per_lead": "cf_YjOZpOvYNAqlXIxPR57WuZ9ddslb8Tfz95qYg9l2WXT",
    }
    
    # Field ID mappings for LeadMaggy - Updated
    leadmaggy_updated_fields = {
        "loan_amount": "cf_vx3xIP1wPckpFHCxqpBkXhOW2Ok7ypXoqUVQM2AWOn5",
        "lead_source": "cf_GzunqohladO4xBmliJtkidxRz7zm9Y9YcGQOnUiZ9MU",
        "lead_magnet_source": "cf_8jbyolNPe0r5dtUKETj9bFJdymLbfEfyrVP1JoUlx6W",
        "company_reg": "cf_l0L7XbGzT58IAD0oUuVqGv9HSPZ6RHX7MZQLl5hZjkm",
        "sic": "cf_GgNVUqtXmR3WpSCMzdnAtz8nWb0dAL5UunIcIfP6bwA",
        "years_of_trading": "cf_qxNmTX93SKRuotDUbToyd9B7EoYitRJNnZhnUsjlFIH",
        "companies_house_url": "cf_ATInjT4pj9FCFLWAI5oV8ko1VP6klqBslf8r0uZalbb",
        "lender": "cf_uzwAA5H1cVg9NA4ZQiwyw7FwH1zTCpiSnaKD4SMYZOC",
        "net_assets": "cf_pR2aY9ZNrvuzhZGXSLkXVPjH5Ps6GtMyV6HG9pQDwl0",
        "profitability": "cf_M0crDfs4qPtuaYU86qvDt7W4juER005MFGV78KD16O8",
        "business_model": "cf_Qw7REJDNWtPmaJBt8lVzTF9IrilxB77seuv2iAx1Bmo",
        "use_of_funds": "cf_fJEsyvFD8VgYB2Lqo9nujSQOvtZh25IN7XmfBdq1nCj",
        "turnover": "cf_MKxdB8xyTLoqICScevI9Zvc7ezl4LzFnMMLWtUFG1VC",
        "company_type": "cf_usvFDc6nLEFWSFIvO6X4UaDQyjeHLQqwykLBUZs8Dkj",
        "company_status": "cf_1u0hpz60oxW2m5rYE4lnFf3dy3h7mJ26Ynb7XsN1D21",
        "external_uuid": "cf_eIU2VF39nhuBEOTwjeRMslWdg9aPmkStVK89yiNZ9V1",
        "pay_per_lead": "cf_clW8ELSmKBpWIZN5SWUNdYnHdlKdMXx45IfMrmLrJXV",
    }
    
    def get_field(db_column: str) -> Any:
        """Try original LeadMaggy field ID first, then LeadMaggy - Updated."""
        if leadmaggy_fields.get(db_column) and custom.get(leadmaggy_fields[db_column]) is not None:
            return custom.get(leadmaggy_fields[db_column])
        if leadmaggy_updated_fields.get(db_column) and custom.get(leadmaggy_updated_fields[db_column]) is not None:
            return custom.get(leadmaggy_updated_fields[db_column])
        return None
    
    # Combine business_model and use_of_funds into single field
    business_model = get_field("business_model") or ""
    use_of_funds = get_field("use_of_funds") or ""
    business_model_use_of_funds = None
    if business_model or use_of_funds:
        parts = [p for p in [business_model, use_of_funds] if p]
        business_model_use_of_funds = " / ".join(parts) if parts else None
    
    return {
        "custom_activity_uuid": custom_activity_uuid,
        "custom_activity_id": activity.get("id"),
        "lead_id": activity.get("lead_id"),
        "loan_amount": _parse_numeric(get_field("loan_amount")),
        "lead_source": get_field("lead_source"),
        "lead_magnet_source": get_field("lead_magnet_source"),
        "company_reg": get_field("company_reg"),
        "sic": get_field("sic"),
        "years_of_trading": _parse_int(get_field("years_of_trading")),
        "companies_house_url": get_field("companies_house_url"),
        "turnover": _parse_numeric(get_field("turnover")),
        "net_assets": _parse_numeric(get_field("net_assets")),
        "profitability": get_field("profitability"),
        "lender": get_field("lender"),
        "business_model_use_of_funds": business_model_use_of_funds,
        "company_type": get_field("company_type"),
        "company_status": get_field("company_status"),
        "pay_per_lead_quotezone": _parse_numeric(get_field("pay_per_lead")),
        "updated_at": datetime.utcnow().isoformat(),
    }


# -----------------------------------------------------------------------------
# Google Sheets Dealsheet Mapping
# -----------------------------------------------------------------------------

SHEET_HEADER_MAP = {
    "YYYY-QX": "yyyy_qx",
    "YYYY-MM": "yyyy_mm",
    "YYYY-WW": "yyyy_ww",
    "Date": "date",
    "Company": "company",
    "Company Name": "company",
    "Lender": "lender",
    "FBX/Funding Bay": "fbx_funding_bay",
    "Closer": "closing_broker",
    "Originator": "originator",
    "RSA": "rsa",
    "IF/Non-IF": "if_non_if",
    "Type": "type",
    "Facility Type": "facility_type",
    "Facility Size": "facility_size",
    "Contract end date": "contract_end_date",
    "Notice period": "notice_period",
    "Service charge": "service_charge",
    "Monthly minimums": "monthly_minimums",
    "Arrangement Fee": "arrangement_fee",
    "Success Fee %": "success_fee_percent",
    "Success Fee Amount": "success_fee_amount",
    "Lender Fee Amount": "lender_fee_amount",
    "Gross Rev": "invoice_amount",
    "Partner Introducer": "partner_introducer",
    "Paid Partner?": "paid_partner",
    "Partner Owner": "partner_owner",
    "Partner Comms - Success %": "partner_comms_success_percent",
    "Partner Comms - Success Amount": "partner_comms_success_amount",
    "Partner Comms - Lender %": "partner_comms_lender_percent",
    "Partner Comms - Lender Amount": "partner_comms_lender_amount",
    "Partner Comms - Total Amount": "partner_comms_total_amount",
    "Net Rev": "net_rev",
    "Lead Source": "lead_source",
    "Campaign": "campaign",
    "Sector": "sector",
    "WW": "week",
    "MM": "month_1",
    "QX": "quarter",
    "YYYY": "year",
}

SHEET_NUMERIC_COLUMNS = {
    "facility_size",
    "success_fee_percent",
    "success_fee_amount",
    "lender_fee_amount",
    "invoice_amount",
    "partner_comms_success_percent",
    "partner_comms_success_amount",
    "partner_comms_lender_percent",
    "partner_comms_lender_amount",
    "partner_comms_total_amount",
    "net_rev",
    "arrangement_fee",
    "service_charge",
    "monthly_minimums",
}


SHEET_DATE_COLUMNS = {"date", "contract_end_date"}


def _parse_sheet_date(value: str) -> Optional[str]:
    """Parse a sheet date cell to ISO format (YYYY-MM-DD) for Postgres.

    The dealsheet uses US month/day/year. US formats are tried before UK/EU
    day/month formats so ambiguous values like 03/05/2025 resolve as March 5.
    """
    if not value:
        return None
    value = str(value).strip()
    if not value:
        return None
    if "T" in value:
        value = value.split("T", 1)[0]

    if value.isdigit():
        serial = int(value)
        if 30000 <= serial <= 60000:
            try:
                return (datetime(1899, 12, 30) + timedelta(days=serial)).strftime(
                    "%Y-%m-%d"
                )
            except (ValueError, OverflowError):
                pass

    for fmt in (
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%m-%d-%Y",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%d.%m.%Y",
    ):
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    logger.warning("Could not parse sheet date: %r", value)
    return None


def _parse_sheet_numeric(value: str):
    """Parse a sheet cell value as float, stripping currency symbols and commas."""
    if not value:
        return None
    cleaned = (
        value.replace("£", "")
        .replace("$", "")
        .replace("€", "")
        .replace(",", "")
        .replace("%", "")
        .strip()
    )
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def sheet_to_row(headers: list[str], values: list[str]) -> dict:
    """Map a Google Sheet row to dealsheet_sync_v2_staging column names."""
    row = {}

    for i, header in enumerate(headers):
        header = header.strip()
        db_col = SHEET_HEADER_MAP.get(header)
        if not db_col:
            db_col = next(
                (col for key, col in SHEET_HEADER_MAP.items() if key.lower() == header.lower()),
                None,
            )
        if not db_col:
            continue
        raw = values[i] if i < len(values) else ""
        value = str(raw).strip() if raw is not None and str(raw).strip() else ""

        if not value:
            row[db_col] = None
            continue

        if db_col in SHEET_NUMERIC_COLUMNS:
            row[db_col] = _parse_sheet_numeric(value)
        elif db_col in ("rsa", "paid_partner"):
            row[db_col] = value.lower() == "yes"
        elif db_col == "year":
            row[db_col] = _parse_int(value)
        elif db_col in SHEET_DATE_COLUMNS:
            row[db_col] = _parse_sheet_date(value)
        else:
            row[db_col] = value

    if row.get("closing_broker") and not row.get("closer"):
        row["closer"] = row["closing_broker"]
    if row.get("invoice_amount") is not None and row.get("gross_rev") is None:
        row["gross_rev"] = row["invoice_amount"]
    if row.get("success_fee_amount") is not None and row.get("success_fee") is None:
        row["success_fee"] = row["success_fee_amount"]
    if row.get("month_1") is not None and row.get("month") is None:
        row["month"] = row["month_1"]

    return row


def normalize_company(name: str) -> str:
    """Normalize company name for matching across sheet and database."""
    if not name:
        return ""
    return " ".join(str(name).strip().lower().split())


def deterministic_uuid(*parts: str) -> str:
    """Build a deterministic UUID v5-style identifier from concatenated parts."""
    data = "|".join(parts)
    digest = hashlib.sha1(data.encode("utf-8")).digest()
    b = bytearray(digest[:16])
    b[6] = (b[6] & 0x0F) | 0x50
    b[8] = (b[8] & 0x3F) | 0x80
    hex_str = b.hex()
    return f"{hex_str[:8]}-{hex_str[8:12]}-{hex_str[12:16]}-{hex_str[16:20]}-{hex_str[20:]}"


def _normalize_date_for_match(value) -> str:
    if not value:
        return ""
    parsed = _parse_sheet_date(str(value))
    return parsed or str(value).strip()


def dealsheet_uuid_for_company(company: str, date: str = "") -> str:
    """Stable dealsheet UUID from normalized company name and optional date."""
    return deterministic_uuid(
        normalize_company(company),
        _normalize_date_for_match(date),
    )


DEALSHEET_UUID_FIELDS = (
    "lender",
    "facility_size",
    "invoice_amount",
    "lender_fee_amount",
    "facility_type",
    "type",
    "partner_introducer",
    "net_rev",
)


def dealsheet_uuid_for_row(mapped: dict) -> Optional[str]:
    """Stable dealsheet UUID from row content so each sheet deal maps 1:1."""
    company = normalize_company(mapped.get("company") or "")
    if not company:
        return None

    parts = [company, _normalize_date_for_match(mapped.get("date"))]
    for field in DEALSHEET_UUID_FIELDS:
        value = mapped.get(field)
        parts.append(str(value).strip() if value is not None else "")

    return deterministic_uuid(*parts)


def resolve_dealsheet_uuid(
    mapped: dict,
    existing_by_company: dict[str, list[dict]],
) -> Optional[str]:
    """
    Match a sheet row to an existing dealsheet_uuid by company (and date when needed).

    Returns None when company is missing. Generates a new UUID when the company
    or company+date combination is not in the database (e.g. after a row was deleted).
    """
    company = normalize_company(mapped.get("company") or "")
    if not company:
        return None

    date = _normalize_date_for_match(mapped.get("date"))
    matches = existing_by_company.get(company, [])

    if not matches:
        return dealsheet_uuid_for_company(company, date)

    for row in matches:
        if _normalize_date_for_match(row.get("date")) == date:
            return row["dealsheet_uuid"]

    return dealsheet_uuid_for_company(company, date)
