# SaaS Revenue & Churn Intelligence — Databricks Asset Bundle

The entire project as deployable, version-controlled code. One command ships the
data pipeline, orchestration job, and dashboard to a Databricks workspace.

## What's in here

```
databricks.yml                       # bundle config + dev/prod targets + variables
resources/
  pipeline.yml                       # medallion pipeline (bronze→silver→gold)
  job.yml                            # 2-hourly: generate messy data → pipeline → refresh
  dashboard.yml                      # AI/BI dashboard (revenue + churn + ML pages)
src/
  pipelines/medallion/transformations/  # the pipeline SQL (with EXPECT data-quality rules)
  notebooks/                         # data generator + serving-table refresh
  dashboards/saas_churn.lvdash.json  # exported dashboard definition
  app/                               # Streamlit app (deployed via `databricks apps`)
  ml/                                # churn model train + batch-score (MLflow + Unity Catalog)
```

## Environments
- **dev** (default): `mode: development` — every resource is prefixed `[dev <you>]`,
  schedules are auto-paused, and outputs go to the **`saas_dev`** schema so they can
  never touch prod tables.
- **prod**: `mode: production`, writes to the `default` schema.

## Commands
```bash
export DATABRICKS_CONFIG_PROFILE=DEFAULT

databricks bundle validate -t dev        # check config (the CI step)
databricks bundle deploy   -t dev        # create the resources (dev-prefixed, isolated)
databricks bundle run medallion -t dev   # run the pipeline
databricks bundle destroy  -t dev        # tear it all down

# promote to prod the same way:
databricks bundle deploy -t prod
```

## Not managed by the bundle (run separately)
- `src/ml/train_churn.py` — trains + registers the churn model (MLflow → Unity Catalog).
- `src/ml/score_churn.py` — batch-scores accounts into `account_churn_scores`.
- The Streamlit app is deployed with `databricks apps deploy` (see `src/app/`).
