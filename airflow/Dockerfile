ARG AIRFLOW_BASE_IMAGE=apache/airflow:3.3.2-python3.12

# ---------------------------------------------------------------- runtime
FROM ${AIRFLOW_BASE_IMAGE} AS runtime

COPY requirements.txt /tmp/requirements.txt
# Re-stating the Airflow version stops pip from up/downgrading Airflow itself.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt

# Bake DuckDB's S3 extension into the image so tasks and the query CLI never
# download it at runtime.
RUN python -c "import duckdb; duckdb.connect().execute('INSTALL httpfs')"

ENV PYTHONPATH=/opt/airflow

# ------------------------------------------------------------------- test
FROM runtime AS test

COPY requirements-dev.txt /tmp/requirements-dev.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements-dev.txt
