import os
import json
import boto3
import requests
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def fetch_youtube_raw(**kwargs):
    # Load YouTube API key from environment
    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        raise ValueError("❌ YOUTUBE_API_KEY is missing. Add it to your .env file.")

    # Replace with your real YouTube video ID, playlist ID, or channel ID
    VIDEO_ID = "dQw4w9WgXcQ"

    # YouTube API URL
    url = (
        "https://www.googleapis.com/youtube/v3/videos"
        "?part=snippet,statistics"
        f"&id={VIDEO_ID}"
        f"&key={api_key}"
    )

    response = requests.get(url)
    data = response.json()

    # Handle YouTube API errors
    if "error" in data:
        raise ValueError(f"❌ YouTube API Error: {data}")

    # S3 filename (timestamped)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    s3_key = f"raw/youtube_{timestamp}.json"

    # Upload to S3
    s3 = boto3.client("s3")
    BUCKET = "youtube-data-dhevin"   

    s3.put_object(
        Bucket=BUCKET,
        Key=s3_key,
        Body=json.dumps(data)
    )

    print(f"✔ Uploaded to s3://{BUCKET}/{s3_key}")


with DAG(
    dag_id="youtube_raw_ingest",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@hourly",     # runs every hour
    catchup=False,
    tags=["youtube", "raw", "s3"]
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_youtube_raw",
        python_callable=fetch_youtube_raw,
        provide_context=True
    )
