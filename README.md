# Serverless Sports Analytics Data Pipeline (AWS)

## Overview

This project implements a **serverless data pipeline on AWS** that ingests, transforms, and analyzes weekly NFL player performance data.

The system collects player statistics, stores raw data in an S3 data lake, transforms it into curated Parquet datasets, and enables SQL-based analytics using **AWS Glue** and **Amazon Athena**.

The goal of this project is to simulate a production-style **data lake architecture** where storage, transformation, and query layers are separated to enable scalable and cost-efficient analytics.

---

## Architecture

The pipeline follows a layered data lake architecture.

