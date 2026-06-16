"""Partner fuzzy matching for resolving partner_id from Close lead data.

Implements deterministic matching with configurable threshold to prevent
partner_id flapping between sync runs.
"""

import logging
import re
from typing import Optional

from rapidfuzz import fuzz, process

from config import Config

logger = logging.getLogger(__name__)


class PartnerMatcher:
    """Fuzzy matcher for resolving partner names to UUIDs."""

    def __init__(self, partners: list[dict]):
        """
        Initialize matcher with partner data.
        
        Args:
            partners: List of dicts with 'uuid' and 'partner_name' keys
        """
        self.partners = partners
        self.partner_map = {p["partner_name"]: p["uuid"] for p in partners}
        self.partner_names = list(self.partner_map.keys())
        self.threshold = Config.PARTNER_MATCH_THRESHOLD
        
        # Build normalized name map for faster lookups
        self._normalized_map = {}
        for name in self.partner_names:
            normalized = self._normalize(name)
            self._normalized_map[normalized] = name
        
        logger.info(f"PartnerMatcher initialized with {len(partners)} partners, threshold={self.threshold}")

    @staticmethod
    def _normalize(name: str) -> str:
        """
        Normalize partner name for matching.
        
        - Lowercase
        - Remove punctuation
        - Collapse whitespace
        - Strip leading/trailing whitespace
        """
        if not name:
            return ""
        
        # Lowercase
        name = name.lower()
        
        # Remove common company suffixes for better matching
        suffixes = [" ltd", " limited", " plc", " llp", " inc", " llc", " corp", " corporation"]
        for suffix in suffixes:
            if name.endswith(suffix):
                name = name[:-len(suffix)]
        
        # Remove punctuation
        name = re.sub(r"[^\w\s]", "", name)
        
        # Collapse whitespace
        name = re.sub(r"\s+", " ", name).strip()
        
        return name

    def match(self, partner_name) -> Optional[str]:
        """
        Find the best matching partner UUID for a given name.
        
        Args:
            partner_name: The partner name to match (string or list)
            
        Returns:
            Partner UUID if match confidence >= threshold, None otherwise
        """
        if not partner_name:
            return None
        
        # Handle list values (take first element)
        if isinstance(partner_name, list):
            partner_name = partner_name[0] if partner_name else None
            if not partner_name:
                return None
        
        # Ensure it's a string
        if not isinstance(partner_name, str):
            partner_name = str(partner_name)
        
        partner_name = partner_name.strip()
        if not partner_name:
            return None
        
        # Try exact match first
        if partner_name in self.partner_map:
            logger.debug(f"Exact match for '{partner_name}'")
            return self.partner_map[partner_name]
        
        # Try normalized exact match
        normalized_input = self._normalize(partner_name)
        if normalized_input in self._normalized_map:
            matched_name = self._normalized_map[normalized_input]
            logger.debug(f"Normalized exact match: '{partner_name}' -> '{matched_name}'")
            return self.partner_map[matched_name]
        
        # Fuzzy match using token set ratio (handles word order differences)
        if not self.partner_names:
            return None
        
        result = process.extractOne(
            normalized_input,
            [self._normalize(n) for n in self.partner_names],
            scorer=fuzz.token_set_ratio,
        )
        
        if result is None:
            return None
        
        matched_normalized, score, index = result
        matched_name = self.partner_names[index]
        
        if score >= self.threshold:
            logger.debug(f"Fuzzy match: '{partner_name}' -> '{matched_name}' (score={score})")
            return self.partner_map[matched_name]
        
        logger.debug(f"No match for '{partner_name}' (best: '{matched_name}' score={score} < threshold={self.threshold})")
        return None

    def match_from_lead(self, lead: dict) -> tuple[Optional[str], Optional[str]]:
        """
        Extract and match partner names from Close lead data.
        
        Looks for partner_introducer field in lead custom fields using Close field IDs.
        
        Args:
            lead: Raw lead data from Close API
            
        Returns:
            Tuple of (partner_id, secondary_partner_id)
        """
        custom = lead.get("custom", {}) or {}
        
        # Primary partner - Partner Introducer field ID from Close
        # lcf_fSb5j0xDXyJiKLwdHPYjvRCdbkpUJJpn9krReNKNOyy = Partner Introducer
        primary_partner_name = custom.get("lcf_fSb5j0xDXyJiKLwdHPYjvRCdbkpUJJpn9krReNKNOyy")
        primary_partner_id = self.match(primary_partner_name)
        
        # Secondary partner - Partner Owner field (need to find the field ID)
        # For now, leaving as None since we don't have the field ID
        secondary_partner_id = None
        
        return primary_partner_id, secondary_partner_id
