import os
import json
import boto3
import psycopg2
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def load_latest_raw_to_postgres():
    s3 = boto3.client("s3")
    bucket = "youtube-data-dhevin"

    # 1. Get the newest RAW file in /raw/
    objs = s3.list_objects_v2(Bucket=bucket, Prefix="raw/")
    latest = max(objs["Contents"], key=lambda x: x["LastModified"])
    key = latest["Key"]

    raw_obj = s3.get_object(Bucket=bucket, Key=key)
    data = json.loads(raw_obj["Body"].read())

    # 2. Extract clean fields from raw YouTube API response
    item = data["items"][0]
    snippet = item["snippet"]
    stats = item["statistics"]

    video_id = item["id"]
    title = snippet["title"]
    published_at = snippet["publishedAt"]
    channel_title = snippet["channelTitle"]
    view_count = stats.get("viewCount", 0)
    like_count = stats.get("likeCount", 0)
    comment_count = stats.get("commentCount", 0)

    # 3. Insert into Postgres warehouse
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO youtube.video_stats 
        (video_id, title, published_at, channel_title, view_count, like_count, comment_count)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (video_id)
        DO UPDATE SET
            title = EXCLUDED.title,
            published_at = EXCLUDED.published_at,
            channel_title = EXCLUDED.channel_title,
            view_count = EXCLUDED.view_count,
            like_count = EXCLUDED.like_count,
            comment_count = EXCLUDED.comment_count,
            updated_at = NOW();
    """, (
        video_id,
        title,
        published_at,
        channel_title,
        view_count,
        like_count,
        comment_count
    ))

    conn.commit()
    cur.close()
    conn.close()

    print(f"Loaded clean data for video {video_id} into Postgres.")


with DAG(
    dag_id="youtube_to_postgres",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@hourly",
    catchup=False
) as dag:

    load_task = PythonOperator(
        task_id="load_raw_to_postgres",
        python_callable=load_latest_raw_to_postgres
    )
