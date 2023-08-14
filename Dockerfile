FROM quay.io/astronomer/astro-runtime:8.8.0
RUN python3 -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip3 install --no-cache-dir dbt-bigquery && deactivate