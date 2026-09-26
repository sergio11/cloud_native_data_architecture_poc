"""
Modern Data Architecture POC - GDPR & Privacy Domain Policies.
Implements GDPR Article 25 (Data protection by design and by default):
- Irreversible cryptographic SHA-256 pseudonymization for email addresses.
- Name masking (First Name + Last Initial).
"""

from __future__ import annotations

import hashlib
from typing import Any


class PrivacyPolicy:
    """Domain service enforcing privacy and GDPR pseudonymization."""

    @staticmethod
    def hash_email(email: str) -> str:
        """Computes deterministic SHA-256 hash of normalized email address."""
        if not email:
            return ""
        normalized = email.strip().lower()
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

    @staticmethod
    def mask_name(full_name: str) -> str:
        """Masks personal full name into 'First N.' format to prevent identification."""
        if not full_name:
            return ""
        tokens = full_name.strip().split()
        if len(tokens) >= 2:
            return f"{tokens[0]} {tokens[1][0]}."
        return tokens[0]

    @classmethod
    def apply_customer_masking(cls, record: dict[str, Any]) -> dict[str, Any]:
        """Returns a new dictionary with PII attributes masked according to Gold tier policy."""
        masked = dict(record)
        if "email" in masked:
            masked["email_hash"] = cls.hash_email(str(masked.pop("email")))
        if "name" in masked:
            masked["name_masked"] = cls.mask_name(str(masked.pop("name")))
        return masked
