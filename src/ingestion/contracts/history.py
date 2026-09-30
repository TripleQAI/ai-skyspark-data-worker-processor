"""Typed point history values observed in the supplied hisread export."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import Field, model_validator

from ingestion.contracts.config import StrictModel


class HistoryObservation(StrictModel):
    point_id: str = Field(min_length=1)
    observed_at: datetime
    source_timestamp: str | None = None
    source_timezone: str | None = None
    source_status: str | None = None
    val_bool: bool | None = None
    val_str: str | None = None
    val_num: Decimal | None = None
    val_na: bool = False

    @model_validator(mode="after")
    def exactly_one_typed_value(self) -> "HistoryObservation":
        if self.observed_at.tzinfo is None:
            raise ValueError("history timestamp must have a timezone")
        if self.val_num is not None and not self.val_num.is_finite():
            raise ValueError("numeric history value must be finite")
        present = sum((
            self.val_bool is not None,
            self.val_str is not None,
            self.val_num is not None,
            self.val_na,
        ))
        if present != 1:
            raise ValueError("history observation needs exactly one typed value")
        return self

    @property
    def value_kind(self) -> str:
        if self.val_bool is not None:
            return "bool"
        if self.val_str is not None:
            return "str"
        if self.val_num is not None:
            return "num"
        return "na"
