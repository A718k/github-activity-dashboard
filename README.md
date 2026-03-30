# GitHub Events Dashboard (Bruin)

This repository ingests GitHub event archives and creates reports in BigQuery.

## Assets

- `assets/staging/raw_archive_github_events.sql`: table backed by BigQuery external JSON with GitHub events.
- `assets/staging/raw_archive_github_events.py`: optional local Python ingestion helper (deprecated for bruin SQL pipeline).
- `assets/reports/github_events_engagement.sql`: top 20 repos with engagement percentage and counts for PRs/issues/comments.
- `assets/reports/github_hourly_distribution.sql`: hourly distribution of event types across the day.

## How to run

1. Enter the pipeline folder:

```sh
cd /home/aligri/projects/dtc_project/bruin/github_events_pipeline
```

2. Run the table materialization (one time / as needed):

```sh
bruin run assets/staging/raw_archive_github_events.sql --start-date 2026-01-01 --end-date 2026-01-31
```

3. Run the report assets:

```sh
bruin run assets/reports/github_events_engagement.sql --start-date 2026-01-01 --end-date 2026-01-31
bruin run assets/reports/github_hourly_distribution.sql --start-date 2026-01-01 --end-date 2026-01-31
```

4. Adjust date range for specific analysis.

## Notes

- The pipeline uses `bigquery-default` connection and expects `final-project-dtc.raw_archive.github_events` as source.
- `.gitignore` includes common Python artifacts (`__pycache__`, `.pyc`) and local env files.

## Next section (TODO)

- Project summary
- Dashboard analysis
- Key metrics and chart definitions
