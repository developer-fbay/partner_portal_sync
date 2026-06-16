"""Configuration loaded from environment variables."""

import os
from urllib.parse import quote_plus

from dotenv import load_dotenv

load_dotenv()


class Config:
    SUPABASE_URL: str = os.environ["SUPABASE_URL"]
    SUPABASE_SERVICE_ROLE_KEY: str = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
    CLOSE_API_KEY: str = os.environ["CLOSE_API_KEY"]
    
    @classmethod
    def get_db_connection_string(cls) -> str:
        """
        PostgreSQL connection for advisory locks.

        Uses the database password (NOT the service role JWT).
        Session mode on port 5432 is required so pg_advisory_lock stays on one connection.
        """
        if database_url := os.getenv("DATABASE_URL"):
            return database_url

        db_password = os.environ.get("SUPABASE_DB_PASSWORD")
        if not db_password:
            raise ValueError(
                "Set SUPABASE_DB_PASSWORD or DATABASE_URL for advisory locks. "
                "Find the password in Supabase Dashboard → Project Settings → Database."
            )

        project_ref = cls.SUPABASE_URL.split("//")[1].split(".")[0]
        encoded_password = quote_plus(db_password)
        pooler_host = os.getenv("SUPABASE_POOLER_HOST", "aws-1-eu-west-2.pooler.supabase.com")

        # Session pooler (5432) — transaction pooler (6543) breaks advisory locks
        return (
            f"postgresql://postgres.{project_ref}:{encoded_password}@"
            f"{pooler_host}:5432/postgres"
        )

    # Close API activity type IDs
    PARTNER_REFERRAL_TYPE_IDS = [
        "actitype_1CKUCsigQLAPoNmDABmjcj",  # GEN1. Referral Upload
        "actitype_0PpighCxVchK68dd8Hknzd",  # API - Referral Upload
    ]
    PARTNER_UPLOAD_TYPE_ID = "actitype_5rvWuLY9CJ1bPIAYUU8wCS"  # GEN2. New Partner Upload
    LEAD_MAGGY_TYPE_IDS = [
        "actitype_7F05YTbEK5kDTySb2WN7de",  # LeadMaggy
        "actitype_7YnfLQNfeZsBTMN3ADcCZf",  # LeadMaggy - Updated
    ]
    
    # Close Smart View ID for Lead Source
    LEAD_SOURCE_SMART_VIEW_ID: str = os.getenv("CLOSE_LEAD_SOURCE_SMART_VIEW_ID", "")
    
    # Close Smart View ID for active Partners
    PARTNERS_SMART_VIEW_ID: str = os.getenv(
        "CLOSE_PARTNERS_SMART_VIEW_ID",
        "save_Pb753YjSWwnqFudAZglEspUDuj4zP8HEK9eXDBMGB0w"
    )

    # Close Smart View IDs for Activity types (used for advanced search)
    # These filter activities to only those matching specific criteria
    ACTIVITY_SMART_VIEW_IDS = {
        # GEN1. Referral Upload smart view
        "actitype_1CKUCsigQLAPoNmDABmjcj": "save_JdVj6fV6fW4zlFCXXDaz5IHUKseATwQzco899P7ESZV",
        # API - Referral Upload smart view
        "actitype_0PpighCxVchK68dd8Hknzd": "save_Hf07nAKiaybkf0kOgTOwYiXI8VLmPnOaz5iZFIrvEsg",
        # GEN2. New Partner Upload smart view
        "actitype_5rvWuLY9CJ1bPIAYUU8wCS": "save_MKLP9BMxQExjEbdAgcPacP7lvNGh4lPCS3PXmEozQri",
    }

    # Partner matching threshold (0-100)
    PARTNER_MATCH_THRESHOLD: int = int(os.getenv("PARTNER_MATCH_THRESHOLD", "80"))
