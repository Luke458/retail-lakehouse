# Source this: `. scripts/env.sh`. Everything Airflow writes stays in the repo.
REPO="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")/.." && pwd)"
export AIRFLOW_HOME="$REPO/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$REPO/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__API__HOST=127.0.0.1
# Ports can be moved to run a second copy beside this one (AIRFLOW_PORT, MAIL_PORT).
export AIRFLOW__API__PORT="${AIRFLOW_PORT:-8088}"  # 8080 is taken on this machine
export AIRFLOW__API__BASE_URL="http://localhost:$AIRFLOW__API__PORT"
export MAIL_PORT="${MAIL_PORT:-1025}"
export AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL=10
# Alerts go to a local SMTP catcher (scripts/mail.sh), saved as files in data/mail.
export AIRFLOW_CONN_SMTP_DEFAULT="smtp://localhost:$MAIL_PORT?disable_ssl=true&disable_tls=true&from_email=airflow%40localhost"
export LAKE_ROOT="$REPO/data"
# The Favorita parquet files (scripts/get-data.sh downloads and verifies them).
export FAVORITA_SOURCE="${FAVORITA_SOURCE:-$REPO/data/source/favorita}"
export PATH="$REPO/.venv/bin:$PATH"
# Spark runs in its own venv (delta-spark's pins conflict with Airflow's constraints)
# on JDK 21 (Spark 4.2 does not run on the system's Java 27).
# In GitHub Actions, actions/setup-java exports JAVA_HOME_21_X64.
export JAVA_HOME="${LAKEHOUSE_JAVA_HOME:-${JAVA_HOME_21_X64:-$HOME/.local/opt/jdk-21.0.13+9}}"
export SPARK_PYTHON="${SPARK_PYTHON:-$REPO/.venv-spark/bin/python}"  # override to test another Spark venv (.venv-glue)
# Spark starts its Python workers with `python3` from PATH unless told otherwise; on a
# machine whose default python3 is another minor version (GitHub's runners moved to 3.14)
# they refuse to run. Pin driver and workers to the Spark venv.
export PYSPARK_PYTHON="$SPARK_PYTHON"
export PYSPARK_DRIVER_PYTHON="$SPARK_PYTHON"
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
