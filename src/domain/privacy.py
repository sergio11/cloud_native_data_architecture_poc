"""Modern Data Architecture POC - GDPR and privacy domain policies.

Irreversible SHA-256 email pseudonymization and name masking.
"""

from __future__ import annotations

import hashlib
from typing import Any

#: Default demo salt for PII pseudonymization. Deployments with real PII must
#: configure ``PII_HASH_SALT`` at the composition root.
DEFAULT_PII_HASH_SALT = "poc-demo-salt-v1"


def pseudonymize_email(email: str, salt: str = DEFAULT_PII_HASH_SALT) -> str:
    """Computes the deterministic salted SHA-256 hex digest of an email address.

    Pure function: the digest covers ``f"{salt}:{normalized_email}"``.
    """
    if not email:
        return ""
    normalized = email.strip().lower()
    return hashlib.sha256(f"{salt}:{normalized}".encode()).hexdigest()


class PrivacyPolicy:
    """Domain service enforcing privacy and GDPR pseudonymization."""

    @staticmethod
    def pseudonymize_email(email: str, salt: str = DEFAULT_PII_HASH_SALT) -> str:
        """Salted deterministic SHA-256 pseudonym for an email address (pure)."""
        return pseudonymize_email(email, salt=salt)

    @staticmethod
    def hash_email(email: str, salt: str = DEFAULT_PII_HASH_SALT) -> str:
        """Alias of :meth:`pseudonymize_email`."""
        return pseudonymize_email(email, salt=salt)

    @staticmethod
    def mask_name(full_name: str) -> str:
        """Masks personal full name into 'First N.' format to prevent identification."""
        if not full_name:
            return ""
        tokens = full_name.strip().split()
        if not tokens:
            # Whitespace-only input: nothing to mask, never IndexError.
            return ""
        if len(tokens) >= 2:
            return f"{tokens[0]} {tokens[1][0]}."
        return tokens[0]

    @classmethod
    def apply_customer_masking(cls, record: dict[str, Any], salt: str = DEFAULT_PII_HASH_SALT) -> dict[str, Any]:
        """Returns a new dictionary with PII attributes masked according to Gold tier policy."""
        masked = dict(record)
        if "email" in masked:
            raw_email = masked.pop("email")
            masked["email_hash"] = cls.hash_email("" if raw_email is None else str(raw_email), salt=salt)
        if "name" in masked:
            raw_name = masked.pop("name")
            masked["name_masked"] = cls.mask_name("" if raw_name is None else str(raw_name))
        return masked
