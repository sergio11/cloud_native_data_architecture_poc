"""
Modern Data Architecture POC - Unit Tests: Domain Entities & Business Policies.
Verifies pure domain rules, GDPR pseudonymization, business rollups, and contracts.
"""

from __future__ import annotations

from src.domain.aggregations import BusinessAggregator
from src.domain.contracts import ContractCompatibilityEvaluator
from src.domain.privacy import PrivacyPolicy


def test_gdpr_privacy_policy():
    # Email Hashing
    hash1 = PrivacyPolicy.hash_email("Alice.Smith@Example.com")
    hash2 = PrivacyPolicy.hash_email("alice.smith@example.com")
    assert hash1 == hash2
    assert len(hash1) == 64  # SHA-256 hex digest length

    # Name Masking
    masked = PrivacyPolicy.mask_name("John Doe")
    assert masked == "John D."
    single = PrivacyPolicy.mask_name("Cher")
    assert single == "Cher"

    # Customer Record Masking
    raw = {"id": 1, "name": "Alice Smith", "email": "alice@example.com", "segment": "basic"}
    processed = PrivacyPolicy.apply_customer_masking(raw)
    assert "email" not in processed
    assert "name" not in processed
    assert "email_hash" in processed
    assert processed["name_masked"] == "Alice S."


def test_privacy_masking_blank_and_none_inputs():
    """Regression: blank/None PII must mask to "" — never crash or hash "None"."""
    assert PrivacyPolicy.mask_name("") == ""
    assert PrivacyPolicy.mask_name("   ") == ""

    processed = PrivacyPolicy.apply_customer_masking({"id": 1, "name": None, "email": None})
    assert processed["name_masked"] == ""
    assert processed["email_hash"] == ""
    assert "None" not in processed["email_hash"]

    processed_blank = PrivacyPolicy.apply_customer_masking({"id": 2, "name": "  ", "email": "  "})
    assert processed_blank["name_masked"] == ""


def test_business_aggregator_daily_sales():
    orders = [
        {"order_date": "2024-09-25", "status": "confirmed", "total_amount": 100.0},
        {"order_date": "2024-09-25", "status": "confirmed", "total_amount": 200.0},
        {"order_date": "2024-09-25", "status": "cancelled", "total_amount": 50.0},
        {"order_date": "2024-09-26", "status": "confirmed", "total_amount": 300.0},
    ]
    rollups = BusinessAggregator.aggregate_daily_sales(orders)
    assert len(rollups) == 3

    confirmed_25 = next(r for r in rollups if r.order_date == "2024-09-25" and r.status == "confirmed")
    assert confirmed_25.total_orders == 2
    assert confirmed_25.gross_revenue == 300.0
    assert confirmed_25.average_order_value == 150.0


def test_contract_backward_compatibility():
    old_fields = [
        {"name": "id", "type": "integer", "required": True},
        {"name": "status", "type": "string", "required": True},
    ]

    # Valid backward evolution: adding an optional field
    valid_new = [
        {"name": "id", "type": "integer", "required": True},
        {"name": "status", "type": "string", "required": True},
        {"name": "comment", "type": "string", "required": False},
    ]
    passed, errs = ContractCompatibilityEvaluator.evaluate_backward_compatibility(old_fields, valid_new)
    assert passed is True
    assert len(errs) == 0

    # Breaking change 1: removing an existing field
    invalid_removed = [
        {"name": "id", "type": "integer", "required": True},
    ]
    passed, errs = ContractCompatibilityEvaluator.evaluate_backward_compatibility(old_fields, invalid_removed)
    assert passed is False
    assert any("removed" in e for e in errs)

    # Breaking change 2: adding a required field
    invalid_req = [
        {"name": "id", "type": "integer", "required": True},
        {"name": "status", "type": "string", "required": True},
        {"name": "country", "type": "string", "required": True},
    ]
    passed, errs = ContractCompatibilityEvaluator.evaluate_backward_compatibility(old_fields, invalid_req)
    assert passed is False
    assert any("required=true" in e for e in errs)
