from apache/airflow:2.6.3-python3.8

WORKDIR ./

COPY requirements.txt .

RUN pip install -r requirements.txt && rm -f requirements.txt