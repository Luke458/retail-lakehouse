# infra/snowflake: a second gold target (OpenTofu)

Snowflake objects for building the dbt gold models on Snowflake as well as
DuckDB. Applied 2026-11-11 on a trial account (AWS ap-southeast-2).

| File | What |
|---|---|
| `main.tf` | database `RETAIL_LAKEHOUSE` with schemas `SILVER` and `GOLD`; warehouse `TRANSFORM_XS` (X-Small, suspends after 60 s, created suspended); resource monitor `RETAIL_LAKEHOUSE_CAP` (10 credits a month: notify 50/75%, suspend 90%, stop 100%); roles `LOADER` and `TRANSFORMER` under `SYSADMIN`; key-pair-only service users `LAKEHOUSE_LOADER` and `LAKEHOUSE_DBT` |
| `grants.tf` | least privilege: `LOADER` creates tables, stages and file formats in `SILVER` only; `TRANSFORMER` reads `SILVER` (a future grant covers every table the loader creates) and creates tables and views in `GOLD` |
| `variables.tf`, `outputs.tf` | database name, warehouse size, auto-suspend, credit cap (validated at 20 or less), the service users' public keys |
| `versions.tf` | provider `snowflakedb/snowflake ~> 2.23`, configured entirely from the environment; local state |

Checked after the apply: a second plan shows no changes; each service user
logs in with its own role; `LOADER` cannot create in `GOLD` and
`TRANSFORMER` cannot create in `SILVER`; `TRANSFORMER` can see a table the
loader created; neither user has a password.

## Use

The connection lives outside the repo, in `~/.config/retail-lakehouse/`
(mode 600): `snowflake.env` (account identifier, admin user, key path, role)
and the key pairs. `scripts/snowflake.sh` puts it in the environment for one
command:

```bash
scripts/snowflake.sh tofu -chdir=infra/snowflake init
scripts/snowflake.sh tofu -chdir=infra/snowflake plan -out=tfplan
scripts/snowflake.sh tofu -chdir=infra/snowflake apply tfplan
scripts/snowflake.sh tofu -chdir=infra/snowflake destroy   # by the last trial day
```

CI runs `tofu validate` here with no credentials (`scripts/ci.sh infra`).

## First apply

- Register the admin key with `ALTER USER … SET RSA_PUBLIC_KEY`. The
  trial's admin user was stored with a lowercase name, so the name must be
  double-quoted (`ALTER USER "name" …`); unquoted, Snowflake uppercases it
  and answers "does not exist or not authorized".
- The provider ignores `SNOWFLAKE_ACCOUNT` (it warns, and v3 will read it as
  a fallback), so the wrapper sets the organisation and account names and
  unsets it.
