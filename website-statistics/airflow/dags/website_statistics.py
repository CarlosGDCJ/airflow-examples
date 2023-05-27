import datetime as dt
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="website_statistics",
    start_date=dt.datetime(2023, 4, 1),
    schedule_interval="@daily",
)

fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command=(
        "mkdir -p /tmp/events && "
        "curl -o /tmp/events/{{ ds }}.json "
        "'http://fastapi:8000/events?"
        "start_date={{ ds }}&"
        "end_date={{ next_ds }}'"
    ),
    dag=dag,
)


def _calculate_stats(**context):
    input_path = context["templates_dict"]["input_path"]
    output_path = context["templates_dict"]["output_path"]

    Path(output_path).parent.mkdir(exist_ok=True)
    events = pd.read_json(input_path, orient="records")
    stats = events.groupby(["userId", "date"]).size().reset_index()
    stats.to_csv(output_path, index=False)


calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    templates_dict={
        "input_path": "/tmp/events/{{ ds }}.json",
        "output_path": "/tmp/stats/{{ ds }}.csv",
    },
    dag=dag,
)

fetch_events >> calculate_stats
