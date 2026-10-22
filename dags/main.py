from airflow import DAG
import pendulum

from datetime import datetime, timedelta
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json
from datawarehouse.dwh import staging_table, core_table
from dataquality.soda import yt_etl_data_quality

#define the local timezone
local_tz = pendulum.timezone("Asia/Kuala_Lumpur")

#Default Args for the DAG
default_args = {
    "owner": "NowhEngineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": ["nowhengineer@gmail.com"],
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(minutes=60),
    "start_date": datetime(2025, 1, 1, tzinfo=local_tz),
    #end_date=datetime(2024, 6, 30, tzinfo=local_tz),
}

#variables
staging_schema = "staging"
core_schema = "core"

with DAG(
    dag_id="produce_json",
    default_args=default_args,
    description="A DAG to produce JSON file of video stats from a YouTube channel",
    schedule="0 14 * * *",
    catchup=False,
) as dag:

    # Define tasks
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extract_video_stats = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extract_video_stats)

    # define task dependencies
    playlist_id >> video_ids >> extract_video_stats >> save_to_json_task

with DAG(
    dag_id="update_db",
    default_args=default_args,
    description="DDAG to process JSON file and insert data into both staging and core schemas",
    schedule="0 15 * * *",
    catchup=False,
) as dag_update:

    # Define tasks
    update_staging = staging_table()
    update_core   = core_table()

 
    # define task dependencies
    update_staging >> update_core

with DAG(
    dag_id="data_quality",
    default_args=default_args,
    description="DAG to check the data quality on both layers in the db",
    schedule="0 16 * * *",
    catchup=False,
) as dag_update:

    # Define tasks
    soda_validate_staging = yt_etl_data_quality(staging_schema)
    soda_validate_core   = yt_etl_data_quality(core_schema)

 
    # define task dependencies
    soda_validate_staging >> soda_validate_core 
