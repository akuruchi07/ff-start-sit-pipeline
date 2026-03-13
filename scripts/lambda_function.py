import os
import json
import io
import hashlib
import logging

import boto3
import pandas as pd

'''Logging Setup'''

logger = logging.getLogger()
logger.setLevel(logging.INFO) # Automatically sends logs to cloudWatch

'''AWS Client Initialization'''
s3 = boto3.client("s3") # Creates S3 Client

'''Variables'''
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'nfl-startsit-ashwin') # Name of S3 Bucket
SOURCE_NAME = os.environ.get('SOURCE_NAME', 'nflverse') # Name of source Dataset
ROW_DROP_THRESHOLD = float(os.environ.get("ROW_DROP_THRESEHOLD", "0.30")) # Max acceptable drop in row count

REQUIRED_COLUMNS = ['player_id', 'player_name', 'season', 'week']

'''Fetching Data'''
#Resposible for pulling data from source
def fetch_data(season: int, week: int) -> pd.DataFrame:
    data = [
        {
            "player_id": "1",
            "player_name": "Player A",
            "season": season,
            "week": week,
            "fantasy_points": 12.4
        },
        {
            "player_id": "2",
            "player_name": "Player B",
            "season": season,
            "week": week,
            "fantasy_points": 18.1
        },
    ]

    return pd.DataFrame(data)

'''Required column validation'''
#Checks to see which required columns are missing
def validate_required_columns(df: pd.DataFrame):
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]

    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    
'''Convert dataframe to CSV bytes'''
#Converts text to bytes because S3 expects binary data
def build_csv_bytes(df: pd.DataFrame):
    buffer = io.StringIO()
    df.to_csv(buffer, index = False)
    return buffer.getvalue().encode("utf-8")

'''Checks for file integrity'''
#Creates Fingerprint of file and changes checksum if file changes
def compute_checksum(file_bytes: bytes) -> str:
    return hashlib.md5(file_bytes).hexdigest()

'''Build S3 key for the previous week'''
#Constructs S3 patch where the previous week's file would exist
def get_previous_week_key(season: int, week: int) -> str | None:
    if week <= 1:
        return None
    prev_week = week - 1

    return (
        f"raw/season={season}/week={prev_week:02d}/"
        f"source={SOURCE_NAME}/player_week_stats_{season}_w{prev_week:02d}.csv"
    )
'''Get row count from previous week's file'''
def get_previous_week_row_count(season: int, week: int) -> int | None:
    prev_key = get_previous_week_key(season, week)
    if not prev_key:
        return None
    
    try:

        # Download file from S3
        response = s3.get_object(
            Bucket=BUCKET_NAME,
            Key=prev_key
        )

        # Load it into pandas
        prev_df = pd.read_csv(response["Body"])

        # Return number of rows
        return len(prev_df)

    except s3.exceptions.NoSuchKey:

        # Previous file doesn't exist yet
        logger.warning(f"No previous week file found at {prev_key}")
        return None

    except Exception as e:

        # Catch unexpected errors
        logger.warning(f"Could not read previous week file: {str(e)}")
        return None
    
'''Validate row count drop'''
#Fails pipeline of too many rows are dropped(Below set Threshold)
def validate_row_drop(current_count: int, previous_count: int | None):
    if previous_count is None:
        logger.info("Skipping row drop because no previous week data exists")
        return
    if previous_count == 0:
        logger.info("Skipping row drop validation because previous week row count is 0")
        return
    
    drop_pct = (previous_count - current_count)/previous_count
    logger.info(
        f"Previous row count: {previous_count}, current row count: {current_count}, drop percentage: {drop_pct:.2%}"
    )
    if drop_pct > ROW_DROP_THRESHOLD:
        raise ValueError(
            f"Row count dropped by {drop_pct:.2%}, exceeding threshold {ROW_DROP_THRESHOLD:.2%}"
        )
    
    '''Lanbda entry point'''
def lambda_handler(event, context):
    season = int(event['season'])
    week = int(event['week'])

    logger.info(f"Starting data ingestion: season={season}, week={week}")

    #Fetch Data
    df = fetch_data(season, week)
    current_row_count = len(df)
    logger.info(f"Fetched {current_row_count} rows")

    #Data Quality Check
    validate_required_columns(df)
    previous_row_count = get_previous_week_row_count(season, week)
    validate_row_drop(current_row_count, previous_row_count)

    #Dataframe to CSV
    csv_bytes = build_csv_bytes(df)

    #Checksum
    checksum = compute_checksum(csv_bytes)

    #S3 Upload
    s3_key = (
        f"raw/season={season}/week={week:02d}/"
        f"source={SOURCE_NAME}/player_week_stats_{season}_w{week:02d}.csv"
    )
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=csv_bytes,
        ContentType="text/csv"
    )
     #Log pipeline metadata
    logger.info(
         json.dumps({
             "season": season,
            "week": week,
            "row_count": current_row_count,
            "previous_row_count": previous_row_count,
            "checksum_md5": checksum,
            "s3_key": s3_key,
            "status": "SUCCESS"
         })
     
    )
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Ingestion completed successfully",
            "season": season,
            "week": week,
            "row_count": current_row_count,
            "checksum_md5": checksum,
            "s3_key": s3_key
        })
    }