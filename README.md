Serverless Sports Analytics Data Pipeline (AWS)
Overview
This project implements a serverless data pipeline on AWS that ingests, transforms, and analyzes weekly NFL player performance data.
The system collects player statistics, stores raw data in an S3 data lake, transforms it into curated Parquet datasets, and enables SQL-based analytics using AWS Glue and Amazon Athena.
The goal of the project is to simulate a production-style data lake architecture where storage, transformation, and query layers are separated to enable scalable and cost-efficient analytics.

Architecture
The pipeline follows a layered data lake architecture:

Data Source (NFL statistics)
        ↓
AWS Lambda (Ingestion)
        ↓
S3 Raw Data Layer
        ↓
AWS Lambda (Curation / Transformation)
        ↓
S3 Curated Layer (Parquet)
        ↓
AWS Glue Data Catalog
        ↓
Amazon Athena (SQL Analytics)
This design separates storage (S3) from compute (Athena) and allows serverless analytics without provisioning databases or infrastructure.

Data Lake Structure
The project uses a structured S3 layout to separate raw and curated data.

nfl-startsit-ashwin/

raw/
   season=YYYY/
       week=WW/
           source=nflverse/

curated/
   table=player_week/
       season=YYYY/
           week=WW/
               player_week_YYYY_wWW.parquet

features/
predictions/
metrics/
Layers
Raw layer
Stores unmodified source data for traceability and reprocessing.
Curated layer
Cleaned and structured datasets stored in Parquet format for efficient analytics.
Feature layer (planned)
Derived features used for modeling and predictions.

Technologies Used
AWS services:
	• Amazon S3
	• AWS Lambda
	• AWS Glue Data Catalog
	• Amazon Athena
Programming & tools:
	• Python
	• Pandas
	• SQL
	• Parquet
	• Boto3

Pipeline Components
1. Data Ingestion
A Lambda function retrieves weekly NFL player statistics and writes them to the raw data layer in S3.
Responsibilities:
	• Pull weekly player statistics
	• Validate required columns
	• Generate row count and checksum
	• Store data in the raw S3 layer
Example output location:

s3://nfl-startsit-ashwin/raw/season=2024/week=03/source=nflverse/

2. Data Curation
A second Lambda function transforms the raw dataset into a clean curated dataset.
Processing steps include:
	• schema validation
	• null handling
	• column normalization
	• conversion to Parquet format
Curated output example:

s3://nfl-startsit-ashwin/curated/table=player_week/season=2024/week=03/
Parquet was chosen because it:
	• reduces storage size
	• improves query performance
	• is optimized for analytics engines like Athena

3. Metadata Catalog
The curated datasets are registered in the AWS Glue Data Catalog, which acts as the metadata layer for the data lake.
Example table:

ff_analytics.player_week_stats
The table references the curated S3 location and allows Athena to query the data directly.

4. Analytics Layer
Amazon Athena enables SQL queries directly against the curated datasets stored in S3.
Example queries include:
Top performing players

SELECT
    player_name,
    position,
    fantasy_points_ppr
FROM ff_analytics.player_week_stats
WHERE season = 2024
AND week = 3
ORDER BY fantasy_points_ppr DESC
LIMIT 20;

Breakout usage detection
Identifies players whose target volume increased week-over-week.

WITH usage_trend AS (
    SELECT
        player_name,
        targets,
        week,
        LAG(targets) OVER (PARTITION BY player_id ORDER BY week) AS prev_targets
    FROM ff_analytics.player_week_stats
)

SELECT
    player_name,
    targets,
    prev_targets,
    targets - prev_targets AS target_change
FROM usage_trend
WHERE prev_targets IS NOT NULL
AND targets - prev_targets >= 3
ORDER BY target_change DESC;

Data completeness validation
Checks for missing values and row counts.

SELECT
    season,
    week,
    COUNT(*) AS total_rows
FROM ff_analytics.player_week_stats
GROUP BY season, week
ORDER BY season, week;

Key Design Principles
Serverless architecture
All compute components are serverless:
	• Lambda handles ingestion and transformation
	• Athena performs analytics queries
This eliminates infrastructure management.

Separation of storage and compute
Data is stored in S3 while Athena performs queries only when needed. This model provides a cost-efficient analytics platform.

Data lake layering
The pipeline separates:

Raw data
Curated datasets
Derived features
Predictions
This makes it easier to reprocess data and maintain reliable analytics workflows.

Future Improvements
Planned enhancements include:
	• Feature engineering layer for predictive modeling
	• Weekly automated pipeline orchestration
	• Player projection model for fantasy decision support
	• Backtesting framework for evaluating model performance
	• Dashboard for visualizing weekly player projections

Project Motivation
This project was built to gain hands-on experience designing and implementing cloud-based data pipelines using AWS services commonly used in production analytics systems.
It demonstrates:
	• data ingestion
	• ETL pipeline design
	• data lake architecture
	• serverless analytics
	• SQL-based exploration of curated datasets

Author
Ashwin Kuruchi
Computational Modeling & Data Analytics
Interested in data engineering, cloud architecture, and sports analytics.
