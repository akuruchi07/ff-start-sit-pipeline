import os
import io
import json
import re
import logging

import boto3
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

BUCKET_NAME = os.environ.get("BUCKET_NAME", "nfl-startsit-ashwin")
SOURCE_NAME = os.environ.get("SOURCE_NAME", "nflverse")
CURATED_TABLE = os.environ.get("CURATED_TABLE", "player_week")

REQUIRED_COLUMNS = [
    "player_id",
    "player_name",
    "season",
    "week"
]

# Define the schema you want in curated
# Key = column name after normalization
# Value = target type category
SCHEMA = {
    "player_id": "string",
    "player_name": "string",
    "season": "int",
    "week": "int",
    "team": "string",
    "position": "string",
    "fantasy_points": "float",
    "passing_yards": "float",
    "rushing_yards": "float",
    "receiving_yards": "float"
}


def build_raw_key(season: int, week: int) -> str:
    return f"raw/season={season}/week={week:02d}/source={SOURCE_NAME}/player_week_stats_{season}_w{week:02d}.csv"


def build_curated_key(season: int, week: int) -> str:
    return (
        f"curated/table={CURATED_TABLE}/"
        f"season={season}/week={week:02d}/"
        f"player_week_{season}_w{week:02d}.parquet"
    )


def normalize_column_name(col: str) -> str:
    """
    Convert messy source column names into lowercase snake_case.
    Example:
      'Player Name' -> 'player_name'
      'Fantasy Points' -> 'fantasy_points'
    """
    col = col.strip().lower()
    col = re.sub(r"[^\w\s]", "", col)   # remove punctuation
    col = re.sub(r"\s+", "_", col)      # spaces -> underscore
    return col


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [normalize_column_name(c) for c in df.columns]
    return df


def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Only cast columns that exist.
    Missing optional columns are ignored for now.
    """
    for col, dtype in SCHEMA.items():
        if col not in df.columns:
            continue

        if dtype == "string":
            df[col] = df[col].astype("string")

        elif dtype == "int":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

        elif dtype == "float":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

    return df


def validate_required_columns(df: pd.DataFrame):
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def validate_partition_values(df: pd.DataFrame, season: int, week: int):
    """
    Make sure the file you are curating actually belongs to the season/week requested.
    """
    if "season" in df.columns:
        season_values = set(df["season"].dropna().astype(int).unique().tolist())
        if season_values and season_values != {season}:
            raise ValueError(f"Season mismatch. Found {season_values}, expected {season}")

    if "week" in df.columns:
        week_values = set(df["week"].dropna().astype(int).unique().tolist())
        if week_values and week_values != {week}:
            raise ValueError(f"Week mismatch. Found {week_values}, expected {week}")


def drop_bad_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop rows missing core identifiers needed downstream.
    """
    before = len(df)
    df = df.dropna(subset=["player_id", "player_name", "season", "week"])
    after = len(df)

    dropped = before - after
    logger.info(f"Dropped {dropped} rows due to missing required fields")

    if after == 0:
        raise ValueError("All rows were dropped after required-field validation")

    return df


def read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    logger.info(f"Reading raw CSV from s3://{bucket}/{key}")
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read()
    return pd.read_csv(io.BytesIO(body))


def write_parquet_to_s3(df: pd.DataFrame, bucket: str, key: str):
    logger.info(f"Writing curated Parquet to s3://{bucket}/{key}")

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream"
    )


def lambda_handler(event, context):
    """
    Expected test event:
    {
      "season": 2024,
      "week": 3
    }
    """
    try:
        season = int(event["season"])
        week = int(event["week"])

        raw_key = build_raw_key(season, week)
        curated_key = build_curated_key(season, week)

        # 1. Read raw CSV
        df = read_csv_from_s3(BUCKET_NAME, raw_key)
        raw_row_count = len(df)
        logger.info(f"Raw row count: {raw_row_count}")

        # 2. Normalize columns
        df = normalize_columns(df)
        logger.info(f"Normalized columns: {df.columns.tolist()}")

        # 3. Validate required columns exist
        validate_required_columns(df)

        # 4. Enforce schema
        df = enforce_schema(df)

        # 5. Validate season/week values
        validate_partition_values(df, season, week)

        # 6. Drop bad rows
        df = drop_bad_rows(df)

        curated_row_count = len(df)

        # 7. Write Parquet
        write_parquet_to_s3(df, BUCKET_NAME, curated_key)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Curation completed successfully",
                "season": season,
                "week": week,
                "input_key": raw_key,
                "output_key": curated_key,
                "raw_row_count": raw_row_count,
                "curated_row_count": curated_row_count,
                "columns": df.columns.tolist()
            })
        }

    except Exception as e:
        logger.exception("Curate Lambda failed")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Curation failed",
                "error": str(e)
            })
        }