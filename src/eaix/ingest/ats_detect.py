"""Detect which applicant tracking system (ATS) a careers page runs on.

Usage: python -m eaix.ingest.ats_detect data/seed/companies.csv
Writes the detected ATS back into the `ats` column.
"""
from __future__ import annotations

import csv
import re
import sys
import time
import urllib.robotparser
from pathlib import Path
from urllib.parse import urlparse

import httpx

USER_AGENT = "accilium-energy-ai-index/0.1 (research; contact: energy-ai-index@accilium.com)"

# Fingerprints: pattern in final URL or HTML -> ATS key. Order matters.
FINGERPRINTS: list[tuple[str, str]] = [
    (r"myworkdayjobs\.com|workday", "workday"),
    (r"successfactors|sapsf|jobs\.sap\.com", "successfactors"),
    (r"softgarden", "softgarden"),
    (r"personio", "personio"),
    (r"dvinci|d\.vinci", "dvinci"),
    (r"rexx-systems|rexx", "rexx"),
    (r"prescreen", "prescreen"),
    (r"umantis|abacus", "umantis"),
    (r"smartrecruiters", "smartrecruiters"),
    (r"onlyfy|jobbase", "onlyfy"),
    (r"greenhouse\.io", "greenhouse"),
]


def allowed(url: str) -> bool:
    parts = urlparse(url)
    rp = urllib.robotparser.RobotFileParser()
    rp.set_url(f"{parts.scheme}://{parts.netloc}/robots.txt")
    try:
        rp.read()
    except Exception:
        return True
    return rp.can_fetch(USER_AGENT, url)


def tdm_reserved(resp: httpx.Response) -> bool:
    """TDM Reservation Protocol: header or meta tag `tdm-reservation: 1`."""
    if resp.headers.get("tdm-reservation", "").strip() == "1":
        return True
    return bool(re.search(r'name=["\']tdm-reservation["\']\s+content=["\']1', resp.text, re.I))


def detect(url: str, client: httpx.Client) -> str:
    if not url:
        return ""
    if not allowed(url):
        return "blocked_robots"
    resp = client.get(url, follow_redirects=True, timeout=20)
    if tdm_reserved(resp):
        return "tdm_reserved"
    haystack = f"{resp.url} {resp.text[:200_000]}"
    for pattern, key in FINGERPRINTS:
        if re.search(pattern, haystack, re.I):
            return key
    return "custom"


def main(path: str) -> None:
    rows = list(csv.DictReader(Path(path).open(encoding="utf-8")))
    with httpx.Client(headers={"User-Agent": USER_AGENT}) as client:
        for row in rows:
            try:
                row["ats"] = detect(row.get("careers_url", ""), client)
            except Exception as exc:  # keep going, log in notes
                row["ats"] = "error"
                row["notes"] = f"{row.get('notes', '')} {type(exc).__name__}".strip()
            print(f"{row['name']}: {row['ats']}")
            time.sleep(1.0)
    if rows:
        with Path(path).open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "data/seed/companies.csv")
