"""Unified posting schema every ATS adapter returns (Bronze layer)."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date

EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")
PHONE = re.compile(r"(\+?\d[\d\s/()-]{6,}\d)")


@dataclass
class Posting:
    company_id: str
    source: str            # ats key or "ba_jobsuche"
    source_id: str         # id in the source system
    url: str
    title: str
    location: str = ""
    published: date | None = None
    employment_type: str = ""
    text: str = ""         # full text, personal data stripped
    fetched: date = field(default_factory=date.today)


def strip_personal(text: str) -> str:
    """Remove emails and phone numbers. Names are removed later via NER in Silver."""
    text = EMAIL.sub("[email]", text)
    return PHONE.sub("[phone]", text)
