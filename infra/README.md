# infra: Azure and Databricks for the lakehouse (OpenTofu)

Code only so far: `tofu validate` passes, and nothing has been planned
against a subscription or applied. The cloud run waits on a subscription and
a budget (Questions in `notes/GOAL.md`).

| File | What |
|---|---|
| `main.tf` | resource group; ADLS Gen2 storage account (no account keys) with `files` and `tables` containers, and a cool tier for old drops; Databricks workspace; the access connector (managed identity) with Storage Blob Data Contributor on the lake |
| `databricks.tf` | Unity Catalog storage credential and external locations; catalog `retail_<env>`, schema `favorita`, external volume `files`; the job `transform-favorita` (bronze → silver → reconcile for each feed, single-node job cluster, wheel from the volume) |
| `variables.tf`, `outputs.tf` | name, environment, region (default `australiaeast`), owner tag, node size, wheel version, alert address; outputs are what Airflow and the jobs need (`TABLE_ROOT`, `LAKE_ROOT`, job id) |
| `versions.tf` | OpenTofu ≥ 1.8, azurerm `~> 5.12`, databricks `~> 1.141`; local state |
| `backend.azurerm.hcl.example` | remote state in its own storage account, locked with a blob lease |

Every resource carries the same tags (`project`, `environment`, `owner`,
`managed_by`, `repo`), and the job cluster carries them too, so its VM cost
lands on the project.

## Use

```bash
export PATH="$HOME/.local/opt/tofu-1.13.8:$PATH"   # installed, checksum and signature verified
cd infra
tofu init
tofu fmt -check && tofu validate

# With a subscription (not done yet):
az login
tofu plan -var subscription_id=... -out plan.bin
tofu apply -target=azurerm_databricks_workspace.this   # first time: the Databricks provider needs the workspace
tofu apply plan.bin
# upload the wheel (scripts/package.sh) to <files_root>/artifacts/, then
# trigger the job with the parameter week=2016-01-05
tofu destroy
```

## Cost note

Estimates to check against the Azure pricing calculator before applying.
They are not measured.

- **Storage:** the Favorita lake is a few GB (raw, bronze, silver, gold), so
  pennies a month on LRS hot, less once old drops tier to cool.
- **Databricks jobs compute:** one weekly run is two feeds × three steps on
  one D4ds_v5 node, about 15-20 minutes including cluster start. Billed as
  the VM plus jobs-compute DBUs (premium tier) for those minutes: a small
  amount per run, at most dollars a month at a weekly cadence.
- **Fixed costs:** the workspace itself has no hourly charge, but its
  managed resource group holds networking for the clusters, and with secure
  cluster connectivity that includes a NAT gateway billed by the hour
  whether or not anything runs. For a short demo this is the main line
  item, which is why the plan is apply, run, `tofu destroy`.
- **Guardrails before applying:** a budget with alerts on the resource
  group, and the job cluster's auto-termination (job clusters end with the
  run).

## Not covered yet

- Airflow in the cloud (it triggers the job; locally it runs the same
  commands).
- dbt on Databricks (`dbt-databricks` against the same models), and the
  verity gate as a job task.
- Networking hardening (VNet injection, private endpoints for the storage
  account), and CI deployment with OIDC federation instead of `az login`.
