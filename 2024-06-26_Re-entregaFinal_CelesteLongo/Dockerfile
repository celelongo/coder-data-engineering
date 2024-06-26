#Dockerfile
FROM apache/airflow:2.9.1-python3.8
USER root
COPY requirements.txt requirements.txt
USER airflow
RUN pip install --no-cache-dir -r requirements.txt