# Source this: `. scripts/env.sh`. Everything Airflow writes stays in the repo.
REPO="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")/.." && pwd)"
export AIRFLOW_HOME="$REPO/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$REPO/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__API__HOST=127.0.0.1
export AIRFLOW__API__PORT=8088            # 8080 is taken on this machine
export AIRFLOW__API__BASE_URL=http://localhost:8088
export AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL=10
# Alerts go to a local SMTP catcher (scripts/mail.sh), saved as files in data/mail.
export AIRFLOW_CONN_SMTP_DEFAULT='smtp://localhost:1025?disable_ssl=true&disable_tls=true&from_email=airflow%40localhost'
export LAKE_ROOT="$REPO/data"
export FAVORITA_SOURCE="${FAVORITA_SOURCE:-$HOME/Projects/retail-qc/data/real/favorita}"
export PATH="$REPO/.venv/bin:$PATH"
