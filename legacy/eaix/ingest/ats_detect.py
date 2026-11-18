"""Find each company's careers page and detect which applicant tracking system (ATS) it runs on.

Usage: python -m eaix.ingest.ats_detect data/seed/companies.csv
Fills `careers_url` (if empty) and `ats`, writes a run report to data/reports/ats_detect.md.
"""
from __future__ import annotations

import csv
import re
import sys
import time
import urllib.robotparser
from datetime import date
from pathlib import Path
from urllib.parse import urljoin, urlparse

import httpx

USER_AGENT = "accilium-energy-ai-index/0.1 (research; contact: energy-ai-index@accilium.com)"

# Fingerprints: pattern in final URL or HTML -> ATS key. Order matters.
FINGERPRINTS: list[tuple[str, str]] = [
    (r"myworkdayjobs\.com|\.wd\d\.myworkday", "workday"),
    (r"successfactors|sapsf\.|jobs\.sap\.com|career\d*\.successfactors", "successfactors"),
    (r"softgarden", "softgarden"),
    (r"personio", "personio"),
    (r"dvinci|d\.vinci", "dvinci"),
    (r"rexx-systems|/rexx|rexx-recruitment", "rexx"),
    (r"prescreen", "prescreen"),
    (r"umantis|refline", "umantis"),
    (r"smartrecruiters", "smartrecruiters"),
    (r"onlyfy|jobbase\.io", "onlyfy"),
    (r"greenhouse\.io", "greenhouse"),
    (r"concludis", "concludis"),
    (r"milchundzucker|beesite", "beesite"),
    (r"jobs\.b-ite\.com|b-ite", "bite"),
    (r"talention|talentlink|lumesse", "talentlink"),
    (r"recruitee", "recruitee"),
]

CAREER_LINK = re.compile(r'href=["\']([^"\']+)["\'][^>]*>([^<]{0,80})<', re.I)
CAREER_WORDS = re.compile(
    r"karriere|\bjobs?\b|jobs\.|stellenangebot|offene[-\s]stellen|stellenmarkt|jobportal|career|bewerb", re.I
)
NOT_CAREER = re.compile(r"baustelle|haltestelle|sammelstelle|schlichtung|ladestelle|servicestelle", re.I)
COMMON_PATHS = ["/karriere", "/jobs", "/karriere/jobs", "/de/karriere", "/career", "/unternehmen/karriere"]


def allowed(url: str, client: httpx.Client) -> tuple[bool, str]:
    """robots.txt check per RFC 9309: 4xx = no rules (allowed), 5xx/unreachable = disallowed.

    Python's stock RobotFileParser treats 401/403 as disallow-all, which is pre-RFC
    and produced false blocks on sites that simply refuse unknown agents for robots.txt.
    """
    parts = urlparse(url)
    robots = f"{parts.scheme}://{parts.netloc}/robots.txt"
    try:
        r = client.get(robots, follow_redirects=True, timeout=15)
    except Exception:
        return False, "robots_unreachable"
    if 400 <= r.status_code < 500:
        return True, f"robots_{r.status_code}"
    if r.status_code >= 500:
        return False, f"robots_{r.status_code}"
    rp = urllib.robotparser.RobotFileParser()
    rp.parse(r.text.splitlines())
    return rp.can_fetch(USER_AGENT, url), "robots_ok"


def tdm_reserved(resp: httpx.Response) -> bool:
    """TDM Reservation Protocol: header or meta tag `tdm-reservation: 1`."""
    if resp.headers.get("tdm-reservation", "").strip() == "1":
        return True
    return bool(re.search(r'name=["\']tdm-reservation["\']\s+content=["\']1', resp.text, re.I))


def find_careers_url(website: str, client: httpx.Client) -> str:
    """Look for a careers link on the homepage, then try common paths."""
    try:
        home = client.get(website, follow_redirects=True, timeout=20)
        for href, label in CAREER_LINK.findall(home.text):
            if NOT_CAREER.search(href) or NOT_CAREER.search(label):
                continue
            if CAREER_WORDS.search(href) or CAREER_WORDS.search(label):
                return urljoin(str(home.url), href)
    except Exception:
        pass
    for path in COMMON_PATHS:
        url = urljoin(website, path)
        try:
            r = client.get(url, follow_redirects=True, timeout=15)
            if r.status_code == 200:
                return str(r.url)
        except Exception:
            continue
    return ""


def detect(url: str, client: httpx.Client) -> tuple[str, str]:
    """Return (ats, final_url). Follows one level of links to find the job list."""
    ok, _ = allowed(url, client)
    if not ok:
        return "blocked_robots", url
    resp = client.get(url, follow_redirects=True, timeout=20)
    if tdm_reserved(resp):
        return "tdm_reserved", str(resp.url)
    haystack = f"{resp.url} {resp.text[:300_000]}"
    for pattern, key in FINGERPRINTS:
        if re.search(pattern, haystack, re.I):
            return key, str(resp.url)
    return "custom", str(resp.url)


def main(path: str) -> None:
    rows = list(csv.DictReader(Path(path).open(encoding="utf-8")))
    report = [f"# ATS detection {date.today().isoformat()}", "", "| Company | Careers URL | ATS |", "| --- | --- | --- |"]
    with httpx.Client(headers={"User-Agent": USER_AGENT}) as client:
        for row in rows:
            try:
                if not row.get("careers_url"):
                    row["careers_url"] = find_careers_url(row["website"], client)
                if row["careers_url"]:
                    row["ats"], row["careers_url"] = detect(row["careers_url"], client)
                else:
                    row["ats"] = "not_found"
            except Exception as exc:
                row["ats"] = "error"
                row["notes"] = type(exc).__name__
            line = f"| {row['name']} | {row['careers_url']} | {row['ats']} |"
            print(line)
            report.append(line)
            time.sleep(1.0)
    with Path(path).open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    counts: dict[str, int] = {}
    for row in rows:
        counts[row["ats"]] = counts.get(row["ats"], 0) + 1
    report += ["", "## Summary", ""] + [f"- {k}: {v}" for k, v in sorted(counts.items(), key=lambda x: -x[1])]
    out = Path("data/reports/ats_detect.md")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(report) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "data/seed/companies.csv")
