docker build -t airflow-rockets .
docker run -dp 8080:8080 -v "${PWD}/dags/:/opt/airflow/dags/" --name airflow airflow-rockets