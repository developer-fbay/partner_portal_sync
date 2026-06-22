"""Match dealsheet company names to leads in Supabase."""

import logging
from typing import Optional

from rapidfuzz import fuzz, process

from config import Config
from mappers import normalize_company

logger = logging.getLogger(__name__)


def normalize_entity_name(name: str) -> str:
    """Normalize a company/lead name for matching (strips Ltd suffixes, punctuation)."""
    import re

    if not name:
        return ""
    text = name.lower()
    suffixes = [
        " ltd",
        " limited",
        " plc",
        " llp",
        " inc",
        " llc",
        " corp",
        " corporation",
    ]
    for suffix in suffixes:
        if text.endswith(suffix):
            text = text[: -len(suffix)]
    return re.sub(r"\s+", " ", re.sub(r"[^\w\s]", "", text)).strip()


class LeadMatcher:
    """Resolve dealsheet company names to lead records."""

    def __init__(self, leads_by_name: dict[str, dict]):
        self.leads_by_name = leads_by_name
        self.keys = list(leads_by_name.keys())
        self.threshold = Config.LEAD_MATCH_THRESHOLD
        self._cache: dict[str, Optional[dict]] = {}

    def clear_cache(self) -> None:
        self._cache.clear()

    def register_lead(self, lead: dict) -> None:
        """Add a lead record to the lookup after it is created or fetched."""
        record = {
            "id": str(lead["id"]),
            "close_lead_id": (lead.get("close_lead_id") or "").strip() or None,
        }
        for raw in (lead.get("lead_name"), lead.get("display_name")):
            for normalized in {
                normalize_entity_name(raw or ""),
                normalize_company(raw or ""),
            }:
                if normalized and normalized not in self.leads_by_name:
                    self.leads_by_name[normalized] = record
                    self.keys.append(normalized)

    def match(self, company: str) -> Optional[dict]:
        if not company:
            return None

        cache_key = company.strip().lower()
        if cache_key in self._cache:
            return self._cache[cache_key]

        for key in (normalize_entity_name(company), normalize_company(company)):
            if key:
                lead = self.leads_by_name.get(key)
                if lead:
                    self._cache[cache_key] = lead
                    return lead

        query = normalize_entity_name(company) or normalize_company(company)
        if query and len(query) >= 4 and self.keys:
            result = process.extractOne(
                query,
                self.keys,
                scorer=fuzz.token_sort_ratio,
            )
            if result and result[1] >= self.threshold:
                lead = self.leads_by_name[result[0]]
                self._cache[cache_key] = lead
                logger.debug(
                    "Fuzzy lead match: %r -> score=%s",
                    company,
                    result[1],
                )
                return lead

        self._cache[cache_key] = None
        return None
