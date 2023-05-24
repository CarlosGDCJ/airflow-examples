import json
import pathlib

import airflow
import requests
import requests.exceptions as requests_exceptions

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


dag = DAG(
    dag_id="download_rocket_launches",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval="@daily",
)

download_launches = BashOperator(
    task_id="download_launches",
    bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
    dag=dag,
)


def _get_pictures():
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)

    with open("/tmp/launches.json") as f:
        launches = json.load(f)
        img_urls = [launch["image"] for launch in launches["results"]]

        for url in img_urls:
            try:
                response = requests.get(url)
                filename = url.split("/")[-1]

                with open(f"/tmp/images/{filename}", "wb") as wf:
                    wf.write(response.content)

                # Prints are captured to Ariflow logs
                print(f"Saved {url} to /tmp/images")
            except requests_exceptions.MissingSchema:
                print(f"Invalid URL: {url}")
            except requests_exceptions.ConnectionError:
                print(f"Could not connect to {url}")


get_pictures = PythonOperator(
    task_id="get_pictures",
    python_callable=_get_pictures,
    dag=dag,
)

notify = BashOperator(
    task_id="notify",
    bash_command='echo "There are now $(ls /tmp/images | wc -l) images."',
    dag=dag,
)

download_launches >> get_pictures >> notify
