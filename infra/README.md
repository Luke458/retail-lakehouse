# infra: Azure and Databricks for the lakehouse (OpenTofu)

Applied once on 2026-11-09: plan, apply, one week through the Databricks
job, `tofu destroy`. Results, problems and cost are in
`notes/week-05.md`.

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

# With a subscription (as run on 2026-11-09):
az login
tofu apply -target=azurerm_databricks_workspace.this -target=azurerm_role_assignment.connector_lake \
  -target=azurerm_role_assignment.deployer_lake -target=azurerm_storage_container.lake \
  -target=azurerm_storage_management_policy.lake -var subscription_id=...   # the Databricks provider needs the workspace first
# open the workspace once from the portal (Launch Workspace): see "First apply" below
tofu apply -var subscription_id=...
# upload one week's raw drops and the wheel (no account keys: --auth-mode login)
az storage blob upload-batch --auth-mode login --account-name <storage_account> -d files \
  --destination-path raw/favorita/sales/week_ending=2016-05-24 -s ../data/raw/favorita/sales/week_ending=2016-05-24
az storage blob upload --auth-mode login --account-name <storage_account> -c files \
  -n artifacts/retail_lakehouse-0.1.0-py3-none-any.whl -f ../dist/retail_lakehouse-0.1.0-py3-none-any.whl
# run the job (Jobs API run-now, job_parameters {"week": "2016-05-24"}), then
tofu destroy -var subscription_id=...
```

## First apply on a new subscription

What got in the way on 2026-11-09, in order:

- **Resource providers** (`Microsoft.Databricks`, `.Compute`, `.Network`,
  `.Storage`, `.ManagedIdentity`) were unregistered: `az provider register`.
- **VM size:** the DDSv5 family had 0 vCPU quota, and `Standard_D4ds_v4`
  had quota but no capacity (`SkuNotAvailable`, so the cluster never
  started). `az vm list-usage` and `az vm list-skus` against
  `/api/2.1/clusters/list-node-types` found sizes with both; the default
  is now `Standard_D4ds_v6`.
- **A personal Microsoft account** (gmail): the workspace API answered
  "Did not find workspace with specified org ID" until the workspace was
  opened once from the portal, and opening its URL directly signed in to the
  wrong tenant. Launching it from the portal fixed both.
- **Uploads:** account keys are off, so the person running tofu gets
  Storage Blob Data Contributor (`grant_deployer_data_access`).

## Cost note

Written before the run, as estimates. The measured run is in
`notes/week-05.md`.

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
