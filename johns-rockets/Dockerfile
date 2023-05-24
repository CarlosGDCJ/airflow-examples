FROM apache/airflow:2.4.3-python3.10

RUN airflow db init && \
airflow users create --username admin --password admin --firstname \
Anonymous --lastname Admin --role Admin --email admin@example.org 

ENTRYPOINT [ "/bin/bash" ]
CMD ["-c", "airflow webserver & airflow scheduler"]