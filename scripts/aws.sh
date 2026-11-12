#!/usr/bin/env bash
# Run a command against the AWS deployment (infra/aws/):
#
#   scripts/aws.sh tofu -chdir=infra/aws plan
#   scripts/aws.sh airflow dags trigger transform_favorita_aws --conf '{"week": "2016-05-24"}'
#
# Credentials: the named profile (default retail-lakehouse, from `aws login` or
# `aws configure sso`), never keys in files. Airflow's aws_retail_lakehouse
# connection uses the same profile. The lake bucket and the Glue job name come
# from OpenTofu's outputs once applied, so nothing account-specific is in the
# repo. CI never runs this.
set -euo pipefail
. "$(dirname "$0")/env.sh"
export AWS_PROFILE="${AWS_PROFILE:-retail-lakehouse}"
export AWS_REGION="${AWS_REGION:-ap-southeast-2}"
export AIRFLOW_CONN_AWS_RETAIL_LAKEHOUSE="aws://?region_name=${AWS_REGION}&profile_name=${AWS_PROFILE}"
if [ -f "$REPO/infra/aws/terraform.tfstate" ]; then
  out() { tofu -chdir="$REPO/infra/aws" output -raw "$1" 2>/dev/null || true; }
  RETAIL_LAKEHOUSE_LAKE_BUCKET="$(out lake_bucket)"
  RETAIL_LAKEHOUSE_GLUE_JOB="$(out glue_job)"
  export RETAIL_LAKEHOUSE_LAKE_BUCKET RETAIL_LAKEHOUSE_GLUE_JOB
fi
exec "$@"
