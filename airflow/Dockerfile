FROM apache/airflow:slim-2.10.2
ADD requirements.txt .
USER root
RUN apt update && apt install -y libpq-dev
USER airflow
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt
