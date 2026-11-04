# retail-lakehouse

Weekly retail drops through an orchestrated lakehouse, with
[verity](../retail-qc) as the quality gate between silver and gold.

Data: Corporación Favorita (Ecuador), the public Kaggle grocery set, cut into
Wednesday-Tuesday weekly drops by a vendor simulator. Public data only.

Status: Week 1 done (orchestration and raw landing): `notes/week-01.md`.
Why it's built this way: `notes/decisions.md`.

## Run it locally

```bash
scripts/setup.sh               # .venv with Airflow 3.3.2 pinned to its constraints
scripts/mail.sh &              # SMTP catcher on :1025; alerts land in data/mail/new/
scripts/airflow.sh             # UI and API on http://localhost:8088 (user admin)
scripts/stop.sh                # stop Airflow if it was started in the background
```

The admin password is in `.airflow/simple_auth_manager_passwords.json.generated`.

Publish drops into `data/inbox/` (the only thing the pipeline reads):

```bash
. scripts/env.sh
python -m retail_lakehouse.drops reference
python -m retail_lakehouse.drops publish 2016-01-05 --count 10
python -m retail_lakehouse.drops publish 2016-03-15 --fault late      # data, no manifest
python -m retail_lakehouse.drops publish 2016-03-15 --fault corrupt --force
```

Tests: `pytest` (landing logic, the simulator, and DAG house rules: every task
retries, every DAG alerts). Lint: `ruff check .`.

## Layout

| Path | What |
|---|---|
| `dags/` | Airflow DAGs: `ingest_favorita` (weekly), `ingest_reference` |
| `src/retail_lakehouse/landing.py` | verify a drop against its manifest; land it to raw idempotently |
| `src/retail_lakehouse/drops.py` | vendor simulator (stands in for the source system) |
| `scripts/` | setup, environment, run and stop |
| `notes/` | weekly goals and design decisions |
| `data/` (gitignored) | inbox, raw zone, mail, logs |
