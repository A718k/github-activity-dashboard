/* @bruin
name: raw_archive.github_events
type: bq.sql
connection: bigquery-default

depends:
  - archive_ingestion

materialization:
  type: table
  strategy: merge

columns:
  - name: id
    type: string
    primary_key: true
  - name: type
    type: string
    update_on_merge: true
  - name: repo_name
    type: string
    update_on_merge: true
  - name: actor_login
    type: string
    update_on_merge: true
  - name: created_at
    type: timestamp
    update_on_merge: true
  - name: date
    type: date
    update_on_merge: true
  - name: hour
    type: integer
    update_on_merge: true
  - name: extracted_at
    type: timestamp
    update_on_merge: true

hooks:
  pre:
    - query: |
        CREATE TABLE IF NOT EXISTS `final-project-dtc.raw_archive.github_events` (
          id STRING NOT NULL,
          `type` STRING,
          repo_name STRING,
          actor_login STRING,
          created_at TIMESTAMP,
          date DATE,
          hour INT64,
          extracted_at TIMESTAMP
        )
        PARTITION BY date;
    - query: |
        CREATE OR REPLACE EXTERNAL TABLE `final-project-dtc.raw_archive.github_events_ext`
        (
          id STRING,
          type STRING,
          repo STRUCT<name STRING>,
          actor STRUCT<login STRING>,
          created_at STRING
        )
        OPTIONS (
          format = 'NEWLINE_DELIMITED_JSON',
          uris = ['gs://raw_github_archive/*.json.gz'],
          ignore_unknown_values = true
        );
@bruin */

SELECT
  CAST(id AS STRING) AS id,
  CAST(type AS STRING) AS type,
  repo.name AS repo_name,
  actor.login AS actor_login,
  TIMESTAMP(created_at) AS created_at,
  DATE(TIMESTAMP(created_at)) AS date,
  EXTRACT(HOUR FROM TIMESTAMP(created_at)) AS hour,
  CURRENT_TIMESTAMP() AS extracted_at
FROM `final-project-dtc.raw_archive.github_events_ext`
WHERE DATE(TIMESTAMP(created_at)) BETWEEN DATE('{{ start_date | date_format("%Y-%m-%d") }}') AND DATE('{{ end_date | date_format("%Y-%m-%d") }}');

