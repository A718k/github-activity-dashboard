/* @bruin
name: raw_archive.github_events_engagement_report
type: bq.sql
connection: bigquery-default

depends:
  - raw_archive.github_events

materialization:
  type: view
@bruin */

SELECT
  repo_name,

  COUNT(*) AS total_events,

  COUNTIF(type = 'PullRequestEvent') AS pr_events,
  COUNTIF(type = 'IssuesEvent') AS issue_events,
  COUNTIF(type = 'IssueCommentEvent') AS issue_comment_events,
  COUNTIF(type = 'PullRequestReviewCommentEvent') AS pr_review_comment_events,

  -- total engagement
  COUNTIF(type IN (
    'PullRequestEvent',
    'IssuesEvent',
    'IssueCommentEvent',
    'PullRequestReviewCommentEvent'
  )) AS engagement_events,

  -- engagement percentage
  ROUND(
    COUNTIF(type IN (
      'PullRequestEvent',
      'IssuesEvent',
      'IssueCommentEvent',
      'PullRequestReviewCommentEvent'
    )) * 100.0 / COUNT(*),
    2
  ) AS engagement_pct

FROM `final-project-dtc.raw_archive.github_events`
GROUP BY repo_name
HAVING engagement_pct < 95
ORDER BY engagement_events DESC
LIMIT 20;
