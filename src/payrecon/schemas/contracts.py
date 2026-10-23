"""Data contracts for PSP input files.

Single source of truth for column names, types and reference values.
Phase 1 milestone 2 will turn these into PySpark StructTypes, so keep this
module free of third-party dependencies.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Field:
    name: str
    dtype: str          # logical type: string | decimal(18,2) | timestamp | date
    mandatory: bool
    description: str


TRANSACTION_FIELDS: tuple[Field, ...] = (
    Field("psp_code", "string", True, "PSP that sent the file"),
    Field("psp_transaction_id", "string", True, "PSP-unique transaction id (business key)"),
    Field("merchant_id", "string", True, "Acquirer merchant id"),
    Field("terminal_id", "string", True, "Terminal / channel id"),
    Field("transaction_type", "string", True, "PURCHASE or REFUND"),
    Field("transaction_timestamp", "timestamp", True, "Event time, ISO-8601 UTC"),
    Field("amount", "decimal(18,2)", True, "Positive amount; sign comes from transaction_type"),
    Field("currency", "string", True, "ISO-4217 code"),
    Field("card_scheme", "string", True, "VISA / MASTERCARD / AMEX"),
    Field("masked_pan", "string", True, "Masked card number, never a full PAN"),
    Field("auth_code", "string", False, "Authorisation code, present when approved"),
    Field("response_code", "string", True, "'00' approved, otherwise declined"),
    Field("entry_mode", "string", True, "CHIP / CONTACTLESS / ECOM / MAGSTRIPE"),
)

SETTLEMENT_FIELDS: tuple[Field, ...] = (
    Field("settlement_id", "string", True, "Unique settlement line id"),
    Field("psp_code", "string", True, "PSP that settled"),
    Field("psp_transaction_id", "string", True, "Joins to the transaction"),
    Field("merchant_id", "string", True, "Acquirer merchant id"),
    Field("settlement_date", "date", True, "Date funds settled"),
    Field("gross_amount", "decimal(18,2)", True, "Signed: refunds negative"),
    Field("interchange_fee", "decimal(18,2)", True, "Fee >= 0"),
    Field("scheme_fee", "decimal(18,2)", True, "Fee >= 0"),
    Field("net_amount", "decimal(18,2)", True, "gross - fees"),
    Field("currency", "string", True, "ISO-4217 code"),
)

VALID_CURRENCIES = frozenset({"EUR", "SEK", "PLN", "GBP", "USD","DKK"})
VALID_TRANSACTION_TYPES = frozenset({"PURCHASE", "REFUND"})
VALID_CARD_SCHEMES = frozenset({"VISA", "MASTERCARD", "AMEX"})
VALID_ENTRY_MODES = frozenset({"CHIP", "CONTACTLESS", "ECOM", "MAGSTRIPE"})
APPROVED_RESPONSE_CODE = "00"

MASKED_PAN_PATTERN = r"^\d{6}\*+\d{4}$"


def field_names(fields: tuple[Field, ...]) -> list[str]:
    return [f.name for f in fields]


def mandatory_field_names(fields: tuple[Field, ...]) -> list[str]:
    return [f.name for f in fields if f.mandatory]
