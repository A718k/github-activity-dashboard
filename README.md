# GitHub Insights: Exploring Developer Activity

Welcome to GitHub Insights, a project built as part of Data Talk Club DE Zoomcamp! This repository showcases a full end-to-end data pipeline that processes GitHub event data. Throughout this project, I applied and strengthened key skills from the course
## Project Objective
The goal of GitHub Insights is to turn raw GitHub activity data into actionable insights while showcasing a complete end-to-end data pipeline built with **Bruin**. In this project, I have:

* Ingested and staged large-scale GitHub event data efficiently in a data lake.
* Transformed and enriched the data in a data warehouse, preparing it for advanced analytics.
* Analyzed developer and repository activity to uncover patterns, trends, and engagement metrics.
* Visualized findings in interactive dashboards, highlighting the most popular repositories and time-of-day activity.

## Architecture / Pipeline Overview

This project implements a modern data pipeline architecture, transforming raw GitHub event data into structured insights through multiple stages. The pipeline is orchestrated using **Bruin**, ensuring reproducibility and clear dependency management.

### Pipeline Flow

```markdown id="mermaid2"
## Data Pipeline Overview

```mermaid
flowchart TD
    subgraph Ingestion
        A[archive_ingestion.py]
    end

    subgraph Storage
        B[GCS Data Lake]
    end

    subgraph Staging
        C[raw_archive_github_events.sql]
        D[BigQuery Table]
    end

    subgraph Reporting
        E[github_events_engagement.sql]
        F[github_hourly_distribution.sql]
    end

    subgraph Dashboard
        G[Looker Studio]
    end

    A --> B --> C --> D --> E --> G
    D --> F --> G


1. Data Ingestion (Data Lake)
    * Raw GitHub Archive data (JSON .gz files) is ingested and stored in Google Cloud Storage (GCS).
    * The data is organized by year/month/day partitions for efficient access and scalability.
2. External Table (BigQuery)
    An external table is created in BigQuery, directly querying raw files from GCS.
3. Staging Layer
    Raw nested JSON data is flattened and cleaned into a structured format.
    Key fields extracted: id, type, repo_name, actor_login, created_at, date, hour (this step standardizes the dataset and prepares it for downstream transformations).
4. Data Warehouse (Core Table)
    * Cleaned data is stored in a partitioned BigQuery table:raw_archive.github_events
    * Incremental loading is handled via MERGE strategy, ensuring efficient updates without duplication.
5. Reports Layer
    Analytical models are built on top of the staging data, including:
      * Repository engagement metrics
      * Hourly activity distribution
6. Visualization Layer
    Final datasets are connected to Looker Studio dashboards.
### Key Technologies Used
* Bruin → pipeline orchestration & asset management
* Google Cloud Storage (GCS) → data lake
* BigQuery → data warehouse & transformations
* SQL → data modeling and analytics
* Looker Studio → dashboarding and visualization
