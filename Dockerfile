FROM apache/airflow:2.10.2-python3.12

USER airflow
RUN pip install --no-cache-dir pymupdf4llm openai requests

ENV AIRFLOW_HOME=/opt/airflow/airflow_home
WORKDIR /opt/airflow
