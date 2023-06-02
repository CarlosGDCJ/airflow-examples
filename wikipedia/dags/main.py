import pendulum
from urllib.request import urlretrieve

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow import DAG


today = pendulum.today("utc")
dag = DAG(
    dag_id="wikipedia_pageviews",
    start_date=today,
    schedule_interval="@hourly",
    template_searchpath="/tmp",
)


def _get_data(logical_date):
    year, month, day, hour, *_ = logical_date.timetuple()
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:02}/"
        f"pageviews-{year}{month:02}{day:02}-"
        f"{hour:02}0000.gz"
    )
    print("Retrieving URL:")
    print("", url, sep="\t")

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
    bash_command="gzip -df /tmp/pageviews.gz",
    dag=dag,
)


def _process_data(pagenames, language, logical_date):
    pageviews = dict.fromkeys(pagenames, 0)
    with open("/tmp/pageviews", "r") as f:
        for line in f:
            domain_code, page_title, view_count, *_ = line.split(" ")
            if domain_code == language and page_title in pagenames:
                pageviews[page_title] = view_count

    with open("/tmp/postgres_query.sql", "w") as f:
        for pagename, view_count in pageviews.items():
            f.write(
                "INSERT INTO pageview_counts VALUES ("
                f"'{pagename}, {view_count}, {logical_date}"
                ");\n"
            )

    print(pageviews)


solar_system = {
    "Sun",
    "Mercury_(planet)",
    "Venus",
    "Earth",
    "Mars",
    "Jupiter",
    "Saturn",
    "Uranus",
    "Neptune",
}
process_data = PythonOperator(
    task_id="process_data",
    python_callable=_process_data,
    op_kwargs={"pagenames": solar_system, "language": "en"},
    dag=dag,
)


write_to_postgres = PostgresOperator(
    task_id="write_to_postgres",
    postgres_conn_id="my_postgres",
    sql="postgres_query.sql",
    dag=dag,
)

get_data >> extract_pageview >> process_data
