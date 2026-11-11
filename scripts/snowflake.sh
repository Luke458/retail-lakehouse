#!/usr/bin/env bash
# Run a command with the Snowflake connection in the environment:
#
#   scripts/snowflake.sh tofu -chdir=infra/snowflake plan
#
# Reads ~/.config/retail-lakehouse/snowflake.env (SNOWFLAKE_ACCOUNT as
# ORGNAME-ACCOUNTNAME, SNOWFLAKE_USER, SNOWFLAKE_PRIVATE_KEY_PATH,
# SNOWFLAKE_ROLE) and the service users' public keys beside it. Nothing
# account-specific lives in the repo, and CI never runs this.
set -euo pipefail
. "$(dirname "$0")/env.sh"
conf="${SNOWFLAKE_CONFIG_DIR:-$HOME/.config/retail-lakehouse}"
[ -r "$conf/snowflake.env" ] || { echo "no $conf/snowflake.env" >&2; exit 2; }
set -a
# shellcheck disable=SC1091
. "$conf/snowflake.env"
set +a
: "${SNOWFLAKE_ACCOUNT:?set in snowflake.env}" "${SNOWFLAKE_USER:?set in snowflake.env}"
pem_body() { grep -v -- '-----' "$1" | tr -d '\n'; }
# The OpenTofu provider's settings.
export SNOWFLAKE_ORGANIZATION_NAME="${SNOWFLAKE_ACCOUNT%%-*}"
export SNOWFLAKE_ACCOUNT_NAME="${SNOWFLAKE_ACCOUNT#*-}"
export SNOWFLAKE_AUTHENTICATOR=SNOWFLAKE_JWT
# The provider warns on SNOWFLAKE_ACCOUNT (v3 will read it); Python code reads this name instead.
export SNOWFLAKE_ACCOUNT_IDENTIFIER="$SNOWFLAKE_ACCOUNT"
unset SNOWFLAKE_ACCOUNT
SNOWFLAKE_PRIVATE_KEY="$(cat "$SNOWFLAKE_PRIVATE_KEY_PATH")"
export SNOWFLAKE_PRIVATE_KEY
# Service users (public keys only; the private keys stay in $conf).
export TF_VAR_loader_rsa_public_key="$(pem_body "$conf/snowflake_loader_key.pub")"
export TF_VAR_dbt_rsa_public_key="$(pem_body "$conf/snowflake_dbt_key.pub")"
export SNOWFLAKE_LOADER_KEY_PATH="$conf/snowflake_loader_key.p8"
export SNOWFLAKE_DBT_KEY_PATH="$conf/snowflake_dbt_key.p8"
exec "$@"
