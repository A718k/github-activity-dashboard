/* @bruin
name: raw_archive.github_hourly_distribution_report
type: bq.sql
connection: bigquery-default

depends:
  - raw_archive.github_events

materialization:
  type: view
@bruin */

SELECT
  EXTRACT(HOUR FROM created_at) AS hour_of_day,
  COUNT(*) AS total_events,
  COUNTIF(type = 'PullRequestEvent') AS pr_events,
  COUNTIF(type = 'IssuesEvent') AS issue_events,
  COUNTIF(type = 'IssueCommentEvent') AS issue_comment_events,
  COUNTIF(type = 'PullRequestReviewCommentEvent') AS pr_review_comment_events
FROM `final-project-dtc.raw_archive.github_events`
GROUP BY hour_of_day
ORDER BY hour_of_day;
