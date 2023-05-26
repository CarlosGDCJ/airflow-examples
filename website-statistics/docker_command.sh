docker build -t airflow-website .
docker run -dp 8080:8080 -v "${PWD}/dags/:/opt/airflow/dags/" --network host --name airflow airflow-website