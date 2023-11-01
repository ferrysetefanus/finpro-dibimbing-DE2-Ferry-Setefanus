FROM apache/airflow:2.6.3

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY ./.env /opt/app/.env