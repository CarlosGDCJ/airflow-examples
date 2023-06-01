import datetime as dt
import pandas as pd
from urllib.request import urlretrieve

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG


today = dt.datetime.today()
dag = DAG(
    dag_id="wikipedia_pageviews",
    start_date=today,
    schedule_interval="@hourly",
)


def _get_data(data_interval_start):
    year, month, day, hour, *_ = data_interval_start.timetuple()
    url = (
        "'https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:02}/"
        f"pageviews-{year}{month:02}{day:02}-"
        f"{hour:02}0000.gz'"
    )

    output_path = "/tmp/pageviews.gz"
    urlretrieve(url, output_path)


# fmt: off
get_data = PythonOperator(
    task_id="get_data",
    python_callable=_get_data,
    dag=dag
)
# fmt: on

extract_pageview = BashOperator(
    task_id="extract_pageview",
    bash_command="gzip -d /tmp/pageview.gz",
    dag=dag,
)


def _process_data(pagenames, language):
    freqs = dict.fromkeys(pagenames, 0)
    with open("/tmp/pageviews", "r") as f:
        for line in f:
            domain_code, page_title, view_count, *_ = line.split(" ")
            if domain_code == language and page_title in freqs:
                freqs[page_title] = view_count

    print(freqs)


solar_system = (
    "Mercury",
    "Venus",
    "Earth",
    "Mars",
    "Jupiter",
    "Saturn",
    "Uranus",
    "Neptune",
)
process_data = PythonOperator(
    task_id="process_data",
    python_callable=_process_data,
    op_kwargs={"pagenames": solar_system, "language": "en"},
    dag=dag,
)

get_data >> extract_pageview >> process_data
