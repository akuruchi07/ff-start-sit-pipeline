import os
import json
import hashlib
import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

BUCKET_NAME = os.environ.get("BUCKET_NAME", "nfl-startsit-ashwin")
SOURCE_NAME = os.environ.get("SOURCE_NAME", "nflverse")
ROW_DROP_THRESHOLD = float(os.environ.get("ROW_DROP_THRESHOLD", "0.30"))

REQUIRED_COLUMNS = ["player_id", "player_name", "season", "week"]


def fetch_data(season, week):
    return [
        {
            "player_id": "1",
            "player_name": "Player A",
            "season": season,
            "week": week,
            "fantasy_points": 12.4,
        },
        {
            "player_id": "2",
            "player_name": "Player B",
            "season": season,
            "week": week,
            "fantasy_points": 18.1,
        },
    ]


def validate_required_columns(rows):
    if not rows:
        raise ValueError("No rows returned from source")

    columns = set(rows[0].keys())
    missing = [col for col in REQUIRED_COLUMNS if col not in columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def build_csv_bytes(rows):
    headers = list(rows[0].keys())
    lines = [",".join(headers)]

    for row in rows:
        values = [str(row.get(col, "")) for col in headers]
        lines.append(",".join(values))

    csv_text = "\n".join(lines) + "\n"
    return csv_text.encode("utf-8")


def get_previous_week_key(season, week):
    if week <= 1:
        return None
    prev_week = week - 1
    return (
        f"raw/season={season}/week={prev_week:02d}/"
        f"source={SOURCE_NAME}/player_week_stats_{season}_w{prev_week:02d}.csv"
    )


def get_previous_week_row_count(season, week):
    prev_key = get_previous_week_key(season, week)
    if not prev_key:
        return None

    try:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=prev_key)
        body = response["Body"].read().decode("utf-8").strip().splitlines()

        if len(body) <= 1:
            return 0

        return len(body) - 1

    except Exception as e:
        logger.warning(f"No previous week file found or could not read it: {str(e)}")
        return None


def validate_row_drop(current_count, previous_count):
    if previous_count is None or previous_count == 0:
        logger.info("Skipping row drop validation.")
        return

    drop_pct = (previous_count - current_count) / previous_count
    logger.info(
        f"Previous row count: {previous_count}, current row count: {current_count}, drop_pct: {drop_pct:.2%}"
    )

    if drop_pct > ROW_DROP_THRESHOLD:
        raise ValueError(
            f"Row count dropped by {drop_pct:.2%}, exceeding threshold of {ROW_DROP_THRESHOLD:.2%}"
        )


def lambda_handler(event, context):
    season = int(event["season"])
    week = int(event["week"])

    logger.info(f"Starting ingestion for season={season}, week={week}")

    rows = fetch_data(season, week)
    current_row_count = len(rows)

    validate_required_columns(rows)

    previous_row_count = get_previous_week_row_count(season, week)
    validate_row_drop(current_row_count, previous_row_count)

    csv_bytes = build_csv_bytes(rows)
    checksum = hashlib.md5(csv_bytes).hexdigest()

    s3_key = (
        f"raw/season={season}/week={week:02d}/"
        f"source={SOURCE_NAME}/player_week_stats_{season}_w{week:02d}.csv"
    )

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=csv_bytes,
        ContentType="text/csv",
    )

    logger.info(
        json.dumps(
            {
                "season": season,
                "week": week,
                "row_count": current_row_count,
                "previous_row_count": previous_row_count,
                "checksum_md5": checksum,
                "s3_key": s3_key,
                "status": "SUCCESS",
            }
        )
    )

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "Ingestion completed successfully",
                "season": season,
                "week": week,
                "row_count": current_row_count,
                "checksum_md5": checksum,
                "s3_key": s3_key,
            }
        ),
    }