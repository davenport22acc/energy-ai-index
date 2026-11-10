# Energy AI Index (accilium)

Public, interactive analysis of AI potential per role in Austrian and German energy companies, based on a meta-analysis of their job postings.

Learning build on Microsoft Fabric: data engineering, data science, agentic coding.

## How it works

1. **Universe**: list of utilities (AT complete, DE largest) in `data/seed/companies.csv`
2. **Ingest**: detect each company's applicant tracking system (ATS), scrape postings via one adapter per ATS, generic LLM fallback for custom sites
3. **Bronze**: raw postings, personal data (names, emails, phones) stripped on ingest
4. **Silver**: deduplicated postings, role mapped to ESCO / ISCO-08, tasks extracted by LLM
5. **Gold**: AI exposure per task (ILO 2025 index, Anthropic Economic Index) aggregated per role and company
6. **Export**: aggregates only, as JSON, to the static website in `web/`

## Rules

- No persons. Only roles and companies.
- No full posting texts on the website, only aggregates and our own scores.
- Respect robots.txt and machine-readable TDM reservations (§ 44b UrhG DE, § 42h UrhG AT). Log per domain.
- Raw texts are kept only as long as needed for the analysis.

## Layout

```
src/eaix/ingest/      ATS detection and adapters
src/eaix/analysis/    role mapping, task extraction, scoring
data/seed/            company universe
fabric/notebooks/     Fabric notebooks (synced via Fabric Git integration)
web/                  public website
docs/                 method and decisions
```

## Phases

- Phase 0 (no Fabric needed): universe AT, ATS fingerprinting, first 3 adapters, local Delta files
- Phase 1: Fabric workspace, Lakehouse, pipelines, Git sync
- Phase 2: role mapping, task scoring, validation (150 hand-labelled tasks, kappa >= 0.7)
- Phase 3: website MVP, approval by management and legal, go-live
- Phase 4: DE breadth, maturity signals (TED tenders), time series

Plan and research: see the Claude Doc "Energy AI Index: Plan und Research".
