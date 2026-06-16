"""Close CRM API client with pagination and exponential backoff."""

import logging
from typing import Any, Generator, Optional
from datetime import datetime

import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential_jitter,
    retry_if_exception_type,
)

from config import Config

logger = logging.getLogger(__name__)

BASE_URL = "https://api.close.com/api/v1"

# Fields to request from POST /data/search/ for lead mapping
LEAD_SEARCH_FIELDS = [
    "id",
    "name",
    "display_name",
    "status_id",
    "status_label",
    "description",
    "url",
    "html_url",
    "date_created",
    "date_updated",
    "contacts",
    "addresses",
    "custom",
]


class RateLimitError(Exception):
    """Raised when Close API returns 429."""
    pass


class CloseClient:
    """Client for Close CRM API with retry logic and pagination."""

    def __init__(self):
        self.api_key = Config.CLOSE_API_KEY
        self._client = httpx.Client(
            base_url=BASE_URL,
            auth=(self.api_key, ""),
            timeout=60.0,
        )

    def close(self):
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    @retry(
        retry=retry_if_exception_type((RateLimitError, httpx.TransportError)),
        wait=wait_exponential_jitter(initial=1, max=60, jitter=5),
        stop=stop_after_attempt(10),
        before_sleep=lambda retry_state: logger.warning(
            f"Rate limited or transport error, retrying in {retry_state.next_action.sleep} seconds..."
        ),
    )
    def _request(self, method: str, endpoint: str, **kwargs) -> dict:
        """Make a request with automatic retry on rate limits."""
        response = self._client.request(method, endpoint, **kwargs)
        
        if response.status_code == 429:
            logger.warning("Close API rate limit hit (429)")
            raise RateLimitError("Rate limited by Close API")
        
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _normalize_search_lead(record: dict) -> dict:
        """Convert a /data/search/ lead record into standard lead API shape."""
        lead = {k: v for k, v in record.items() if k != "__object_type"}

        custom = dict(lead.get("custom") or {})
        for key, value in record.items():
            if key.startswith("custom."):
                custom[key.removeprefix("custom.")] = value

        if custom:
            lead["custom"] = custom

        return lead

    def _build_saved_search_query(
        self,
        saved_search_id: str,
        date_updated_gte: Optional[datetime] = None,
    ) -> dict:
        """Build Advanced Filtering query for a saved smart view."""
        saved_search = {"type": "saved_search", "saved_search_id": saved_search_id}

        if not date_updated_gte:
            return saved_search

        return {
            "type": "and",
            "queries": [
                saved_search,
                {
                    "type": "field_condition",
                    "field": {
                        "type": "regular_field",
                        "object_type": "lead",
                        "field_name": "date_updated",
                    },
                    "condition": {
                        "type": "moment_range",
                        "on_or_after": {
                            "type": "fixed_utc",
                            "value": date_updated_gte.strftime("%Y-%m-%dT%H:%M:%S"),
                        },
                    },
                },
            ],
        }

    def get_leads_from_smart_view(
        self,
        smart_view_id: str,
        date_updated_gte: Optional[datetime] = None,
    ) -> Generator[dict, None, None]:
        """
        Fetch leads from a saved smart view via POST /data/search/.

        GET /lead/?smart_view_id=... does NOT apply the view's filters (returns
        all org leads). Using saved_search in Advanced Filtering returns the
        same ~8k filtered set shown in the Close UI.
        """
        body: dict[str, Any] = {
            "query": self._build_saved_search_query(smart_view_id, date_updated_gte),
            "limit": 200,
            "include_counts": True,
            "_fields": {"lead": LEAD_SEARCH_FIELDS},
            "sort": [
                {
                    "direction": "desc",
                    "field": {
                        "field_name": "date_updated",
                        "object_type": "lead",
                        "type": "regular_field",
                    },
                }
            ],
        }

        cursor = None
        total_fetched = 0
        total_expected: Optional[int] = None

        while True:
            if cursor:
                body["cursor"] = cursor
            elif "cursor" in body:
                del body["cursor"]

            logger.debug(f"Searching leads via saved search, cursor={cursor}")
            data = self._request("POST", "/data/search/", json=body)

            if total_expected is None:
                count = data.get("count") or {}
                total_expected = count.get("total")
                logger.info(
                    f"Smart view {smart_view_id}: {total_expected} matching leads"
                )

            for record in data.get("data", []):
                if record.get("__object_type") != "lead":
                    continue
                yield self._normalize_search_lead(record)
                total_fetched += 1

            cursor = data.get("cursor")
            if not cursor:
                break

            if total_expected is not None and total_fetched >= total_expected:
                break

        logger.info(f"Fetched {total_fetched} leads total")

    def get_lead_by_id(self, lead_id: str) -> Optional[dict]:
        """Fetch a single lead by its Close ID."""
        try:
            return self._request("GET", f"/lead/{lead_id}/")
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise

    def get_activities(
        self,
        date_created_gte: Optional[datetime] = None,
        custom_activity_type_id: Optional[list[str]] = None,
    ) -> Generator[dict, None, None]:
        """
        Fetch custom activities with offset pagination via GET /activity/.
        
        DEPRECATED: Use get_activities_via_smart_views() for better performance.
        This method scans ALL activities globally and filters client-side.
        
        Paginates ascending by date_created for stable offset pagination.
        """
        type_filter = set(custom_activity_type_id or [])
        params: dict[str, Any] = {
            "_limit": 100,
            "_skip": 0,
            "_order_by": "date_created",
        }
        
        if date_created_gte:
            params["date_created__gte"] = date_created_gte.isoformat()

        total_matched = 0

        while True:
            logger.debug(f"Fetching activities page, skip={params['_skip']}")
            data = self._request("GET", "/activity/", params=params)
            
            activities = data.get("data", [])
            if not activities:
                break
                
            for activity in activities:
                activity_type_id = activity.get("custom_activity_type_id")
                if type_filter and activity_type_id not in type_filter:
                    continue

                activity_id = activity.get("id")
                if activity_id:
                    activity = self._request("GET", f"/activity/custom/{activity_id}/")

                yield activity
                total_matched += 1

            if len(activities) < params["_limit"]:
                break
            
            params["_skip"] += params["_limit"]

        logger.info(f"Fetched {total_matched} matching custom activities")

    def get_activities_via_smart_views(
        self,
        activity_type_smart_views: dict[str, str],
        date_updated_gte: Optional[datetime] = None,
    ) -> Generator[tuple[dict, dict], None, None]:
        """
        Fetch activities by iterating through leads from activity-specific smart views.
        
        Paginates each smart view fully before fetching per-lead activities so
        search cursors are not held open across slow per-lead API calls.
        
        Args:
            activity_type_smart_views: Dict mapping activity_type_id -> saved_search_id
            date_updated_gte: Optional date filter for incremental sync
            
        Yields:
            Tuple of (activity_dict, lead_dict) for each activity found
        """
        total_leads = 0
        total_activities = 0
        
        for activity_type_id, smart_view_id in activity_type_smart_views.items():
            logger.info(f"Fetching activities via smart view for type {activity_type_id}")

            # Paginate smart view quickly first — cursors expire if held during
            # slow per-lead activity fetches.
            leads = list(
                self.get_leads_from_smart_view(
                    smart_view_id=smart_view_id,
                    date_updated_gte=date_updated_gte,
                )
            )
            activities_for_type = 0

            for lead in leads:
                lead_id = lead.get("id")
                total_leads += 1

                activities = self.get_lead_activities(
                    lead_id=lead_id,
                    custom_activity_type_id=[activity_type_id],
                )

                for activity in activities:
                    yield activity, lead
                    activities_for_type += 1
                    total_activities += 1

            logger.info(
                f"Type {activity_type_id}: {len(leads)} leads, "
                f"{activities_for_type} activities"
            )
        
        logger.info(
            f"Smart view activity fetch complete: "
            f"{total_leads} leads checked, {total_activities} activities found"
        )

    def get_lead_activities(
        self,
        lead_id: str,
        custom_activity_type_id: Optional[list[str]] = None,
    ) -> list[dict]:
        """
        Fetch all custom activities for a specific lead.
        
        Args:
            lead_id: The Close lead ID
            custom_activity_type_id: Optional list of activity type IDs to filter
            
        Returns:
            List of activity dictionaries sorted by date_created descending
        """
        params = {
            "_limit": 100,
            "lead_id": lead_id,
            "_order_by": "-date_created",  # Descending to get latest first
        }
        
        if custom_activity_type_id:
            params["custom_activity_type_id__in"] = ",".join(custom_activity_type_id)

        activities = []
        skip = 0

        while True:
            params["_skip"] = skip
            data = self._request("GET", "/activity/custom/", params=params)
            
            batch = data.get("data", [])
            if not batch:
                break
                
            activities.extend(batch)

            if len(batch) < params["_limit"]:
                break
            
            skip += params["_limit"]

        return activities

    def get_latest_lead_maggy_activity(self, lead_id: str) -> Optional[dict]:
        """
        Fetch the latest LeadMaggy activity for a lead.
        
        Returns the most recent activity by date_created across both LeadMaggy type IDs.
        """
        activities = self.get_lead_activities(
            lead_id=lead_id,
            custom_activity_type_id=Config.LEAD_MAGGY_TYPE_IDS,
        )
        
        if not activities:
            return None
        
        # Already sorted descending by date_created, so first is latest
        return activities[0]
