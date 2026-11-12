# infra/aws: the transform on AWS Glue (OpenTofu)

Applied once on 2026-11-12: apply, one week through Glue from Airflow,
an Athena check, `tofu destroy`. Results, problems and cost are in
`notes/week-08-aws.md`.

| File | What |
|---|---|
| `main.tf` | lake bucket (versioned, public access blocked, SSE-S3, owner enforced, old versions expired after a day); Athena workgroup with a 1 GiB scan cutoff and its results bucket (7-day expiry); Glue database |
| `glue.tf` | the Glue entry script in S3; a log group (7-day retention); one Glue 6.0 job, G.1X × 2, 30-minute timeout, no retries, up to 4 concurrent runs, Delta enabled, the wheel and DuckDB installed per run |
| `iam.tf` | the Glue role: read and write the lake bucket, write its own logs and Glue metrics; nothing else |
| `glue_entry.py` | the job's script; the logic is `retail_lakehouse.glue` in the wheel |
| `variables.tf`, `outputs.tf` | region (default `ap-southeast-2`), names, workers (validated 2-4), timeout, Athena limit; outputs are what Airflow and the jobs need |
| `versions.tf` | OpenTofu ≥ 1.8, aws `~> 6.73`, credentials from the environment, default tags on everything, local state |

## Use

```bash
aws login --profile retail-lakehouse --region ap-southeast-2   # temporary credentials, no keys
scripts/aws.sh tofu -chdir=infra/aws init
scripts/aws.sh tofu -chdir=infra/aws plan -out=tfplan
scripts/aws.sh tofu -chdir=infra/aws apply tfplan
scripts/package.sh dist
scripts/aws.sh bash -c 'aws s3 cp dist/retail_lakehouse-0.1.0-py3-none-any.whl "s3://$RETAIL_LAKEHOUSE_LAKE_BUCKET/files/artifacts/"'
scripts/aws.sh scripts/airflow.sh &   # Airflow with the AWS connection and the OpenTofu outputs
scripts/aws.sh airflow dags unpause transform_favorita_aws
scripts/aws.sh airflow dags trigger transform_favorita_aws --conf '{"week": "2016-05-24"}'
scripts/aws.sh tofu -chdir=infra/aws destroy
```

CI runs `tofu validate` here with no credentials (`scripts/ci.sh infra`).

## Notes from the first apply

- Glue creates `/aws-glue/jobs/error`, `/aws-glue/jobs/logs-v2` and
  `/aws-glue/jobs/output` on its first run, outside this state. Delete them
  after a destroy.
- A concurrency limit of exactly the number of parallel steps (2) is too
  tight: Glue counts a finished run for a short while. See
  `notes/week-08-aws.md`.
- `s3://` URIs work as `TABLE_ROOT` on Glue 6.0, where S3A is the only S3
  connector.
