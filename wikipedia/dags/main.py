import pendulum
from urllib.request import urlretrieve

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow import DAG


today = pendulum.datetime(2023, 6, 4)
dag = DAG(
    dag_id="wikipedia_pageviews",
    start_date=today,
    schedule_interval="@hourly",
    template_searchpath="/tmp",
    max_active_runs=1,  # at each run we overwrite the same files
)


def _get_data(logical_date):
    # pageviews...1900.gz (19h) contains data from the interval 19h~20h
    # so when the 20h task runs, we look for pageviews...1900
    year, month, day, hour, *_ = (logical_date - pendulum.duration(hours=1)).timetuple()
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:02}/"
        f"pageviews-{year}{month:02}{day:02}-"
        f"{hour:02}0000.gz"
    )
    output_path = f"/tmp/pageviews.gz"

    print("Retrieving URL:")
    print("", url, sep="\t")
    print("Saving to path:")
    print("", output_path, sep="\t")

    # https://stackoverflow.com/questions/37748105/how-to-use-progressbar-module-with-urlretrieve
    def show_progress(block_num, block_size, total_size):
        print(round(block_num * block_size / total_size * 100, 2))

    urlretrieve(url, output_path, show_progress)


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
    with open(f"/tmp/pageviews", "r") as f:
        for line in f:
            domain_code, page_title, view_count, *_ = line.split(" ")
            if domain_code == language and page_title in pagenames:
                pageviews[page_title] = view_count

    with open(f"/tmp/postgres_query.sql", "w") as f:
        for pagename, view_count in pageviews.items():
            # insert using end of interval as timestamp
            # 20h = data from 19h to 20h
            f.write(
                "INSERT INTO pageview_counts VALUES ("
                f"'{pagename}', {view_count}, '{logical_date}'"
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

get_data >> extract_pageview >> process_data >> write_to_postgres
