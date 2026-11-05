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
# Spark runs in its own venv (delta-spark's pins conflict with Airflow's constraints)
# on JDK 21 (Spark 4.2 does not run on the system's Java 27).
# In GitHub Actions, actions/setup-java exports JAVA_HOME_21_X64.
export JAVA_HOME="${LAKEHOUSE_JAVA_HOME:-${JAVA_HOME_21_X64:-$HOME/.local/opt/jdk-21.0.13+9}}"
export SPARK_PYTHON="$REPO/.venv-spark/bin/python"
# dbt: project in dbt/, everything it writes under data/dbt/.
export DBT_PROJECT_DIR="$REPO/dbt"
export DBT_PROFILES_DIR="$REPO/dbt"
export DBT_TARGET_PATH="$LAKE_ROOT/dbt/target"
export DBT_LOG_PATH="$LAKE_ROOT/dbt/logs"
export DBT_PACKAGES_INSTALL_PATH="$LAKE_ROOT/dbt/packages"
export DBT_SEND_ANONYMOUS_USAGE_STATS=False
# verity (the gate) and dbt each have their own venv too.
export QC_PYTHON="$REPO/.venv-qc/bin/python"
export DBT="$REPO/.venv-dbt/bin/dbt"
# OpenTofu for infra/ (user-space install; in CI, opentofu/setup-opentofu puts it on PATH).
# (An if, not `[ ] &&`: as the last line, a false test would fail every `set -e` script sourcing this.)
if [ -x "$HOME/.local/opt/tofu-1.13.8/tofu" ]; then export PATH="$HOME/.local/opt/tofu-1.13.8:$PATH"; fi
