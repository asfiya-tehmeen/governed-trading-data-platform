FROM apache/airflow:2.10.5-python3.12

COPY --chown=airflow:root pyproject.toml README.md /opt/gtdp/
COPY --chown=airflow:root src /opt/gtdp/src
COPY --chown=airflow:root contracts /opt/gtdp/contracts
COPY --chown=airflow:root dbt /opt/gtdp/dbt
RUN pip install --no-cache-dir "/opt/gtdp[dbt]"

ENV GTDP_CONTRACTS_DIR=/opt/gtdp/contracts \
    DBT_PROFILES_DIR=/opt/gtdp/dbt
