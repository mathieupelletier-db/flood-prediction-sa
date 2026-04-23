# Deployment record - Azure workspace `adb-7405618598462441`

Deployed by `mathieu.pelletier@databricks.com` against
`https://adb-7405618598462441.1.azuredatabricks.net`.

## Bundle deploy command

```bash
databricks bundle deploy -t dev -p adb-7405618598462441 \
  --var="catalog=workspace" \
  --var="schema=mpelletier_flood_montreal_dev" \
  --var="warehouse_id=0334a8e49aac8b19" \
  --var="model_name=workspace.mpelletier_flood_montreal_dev.flood_rf"
```

> `dev` target auto-prefixes everything with `dev_mathieu_pelletier_`, so the
> actual resources land in `workspace.dev_mathieu_pelletier_mpelletier_flood_montreal_dev`.

## Deployed resources

| Kind              | Name (dev-prefixed)                                                      | URL |
|-------------------|--------------------------------------------------------------------------|-----|
| Schema            | `workspace.dev_mathieu_pelletier_mpelletier_flood_montreal_dev`          | [Catalog Explorer](https://adb-7405618598462441.1.azuredatabricks.net/explore/data/workspace/dev_mathieu_pelletier_mpelletier_flood_montreal_dev) |
| Registered Model  | `.../dev_mathieu_pelletier_flood_rf`                                     | [Model page](https://adb-7405618598462441.1.azuredatabricks.net/explore/data/models/workspace/dev_mathieu_pelletier_mpelletier_flood_montreal_dev/dev_mathieu_pelletier_flood_rf) |
| Job               | `[dev mathieu_pelletier] [dev] Flood Prediction Pipeline - greater_montreal` | [Job 429145769088901](https://adb-7405618598462441.1.azuredatabricks.net/jobs/429145769088901) |
| App (running)     | `flood-prediction-dev`                                                   | https://flood-prediction-dev-7405618598462441.1.azure.databricksapps.com |

## What works right now

- App is live and serves the deck.gl SPA.
- `/api/health` returns the configured catalog/schema.
- Bundle/app re-deploys work with `databricks bundle deploy` + `databricks bundle run flood_app`.

## What still needs to happen before the pipeline run shows data

The data tables (`gold_h3_flood_predictions` etc.) are empty — the job hasn't
run yet. Two preconditions specific to this workspace:

### 1. Create a managed Volume for GeoBrix + raw downloads

The Terraform volume creation path failed because the `workspace` catalog's
storage binding on this particular metastore is in a broken state
(`External Location 'abfss://fevm-default-container@sttozmetc6qwenuc...'
does not exist`). Create the volume via the SQL Statement API instead - any
schema owner can do it once the schema exists:

```bash
databricks api post /api/2.0/sql/statements -p adb-7405618598462441 --json '{
  "warehouse_id": "0334a8e49aac8b19",
  "statement": "CREATE VOLUME IF NOT EXISTS workspace.dev_mathieu_pelletier_mpelletier_flood_montreal_dev.raw",
  "wait_timeout": "30s"
}'
```

> If that returns the same "External Location does not exist" error, the
> workspace catalog owner (`toz.ozturk@databricks.com`) needs to re-bind
> default storage via the Account Console, **or** use a different catalog
> with a healthy storage root. In the latter case re-run `bundle deploy`
> with `--var="catalog=<other_catalog>"`.

### 2. Upload GeoBrix v0.2.0 artifacts + init script into that volume

```bash
VOL=/Volumes/workspace/dev_mathieu_pelletier_mpelletier_flood_montreal_dev/raw/geobrix
TMP=$(mktemp -d)
cd "$TMP"
curl -LO https://github.com/databrickslabs/geobrix/releases/download/v0.2.0/geobrix-0.2.0-jar-with-dependencies.jar
curl -LO https://github.com/databrickslabs/geobrix/releases/download/v0.2.0/geobrix-0.2.0-py3-none-any.whl
curl -LO https://github.com/databrickslabs/geobrix/releases/download/v0.2.0/libgdalalljni.so
curl -LO https://raw.githubusercontent.com/databrickslabs/geobrix/master/scripts/geobrix-gdal-init.sh

# Rewrite VOL_DIR inside the init script to point at the target volume
sed -i.bak "s|VOL_DIR=.*|VOL_DIR=\"$VOL\"|" geobrix-gdal-init.sh

for f in geobrix-0.2.0-jar-with-dependencies.jar geobrix-0.2.0-py3-none-any.whl libgdalalljni.so geobrix-gdal-init.sh; do
  databricks fs cp "$f" "dbfs:$VOL/$f" --overwrite -p adb-7405618598462441
done
```

### 3. Run the pipeline

```bash
databricks bundle run flood_pipeline -t dev -p adb-7405618598462441 \
  --var="catalog=workspace" --var="schema=mpelletier_flood_montreal_dev" \
  --var="warehouse_id=0334a8e49aac8b19" \
  --var="model_name=workspace.mpelletier_flood_montreal_dev.flood_rf"
```

First run provisions the DBR 17.3 job cluster, runs the init script (installs
GDAL + GeoBrix JAR), then executes `01_ingest` -> `02_silver_geobrix` ->
`03_gold_features_labels` -> `04_train_and_score`. Total wall time is roughly
10-20 minutes depending on the SRTM + Open-Meteo fetch.

Once the scored table exists, the app immediately starts showing data - no
redeploy needed.
