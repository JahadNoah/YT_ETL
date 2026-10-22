from airflow import DAG
import pendulum

from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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
) as dag_produce:

    # Define tasks
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extract_video_stats = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extract_video_stats)

    # starts update_db as soon as the JSON file is saved
    trigger_update_db = TriggerDagRunOperator(
        task_id="trigger_update_db",
        trigger_dag_id="update_db",
    )

    # define task dependencies
    playlist_id >> video_ids >> extract_video_stats >> save_to_json_task >> trigger_update_db

with DAG(
    dag_id="update_db",
    default_args=default_args,
    description="DAG to process JSON file and insert data into both staging and core schemas",
    schedule=None,  # no clock: produce_json triggers it
    catchup=False,
) as dag_update:

    # Define tasks
    update_staging = staging_table()
    update_core   = core_table()

    # starts data_quality once both tables are updated
    trigger_data_quality = TriggerDagRunOperator(
        task_id="trigger_data_quality",
        trigger_dag_id="data_quality",
    )

    # define task dependencies
    update_staging >> update_core >> trigger_data_quality

with DAG(
    dag_id="data_quality",
    default_args=default_args,
    description="DAG to check the data quality on both layers in the db",
    schedule=None,  # no clock: update_db triggers it
    catchup=False,
) as dag_quality:

    # Define tasks
    soda_validate_staging = yt_etl_data_quality(staging_schema)
    soda_validate_core   = yt_etl_data_quality(core_schema)

 
    # define task dependencies
    soda_validate_staging >> soda_validate_core 
