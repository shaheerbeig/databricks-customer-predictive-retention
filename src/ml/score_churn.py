"""Batch-score CURRENT (non-churned) accounts using the model we registered in
Unity Catalog. The key MLflow idea: we load the model BY NAME+VERSION from the
registry — not from a file path — which is exactly why registering it mattered.
"""
import mlflow
import pandas as pd
from databricks.connect import DatabricksSession

CATALOG, SCHEMA = "ai_dev_kit_testing", "default"
MODEL_URI = f"models:/{CATALOG}.{SCHEMA}.account_churn_model/1"   # <-- name + version, from the UC registry

spark = DatabricksSession.builder.serverless(True).getOrCreate()
mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")

# 1) pull the trained model straight out of the Unity Catalog registry
model = mlflow.sklearn.load_model(MODEL_URI)
print(f"Loaded model from registry: {MODEL_URI}")

# 2) score the accounts that are STILL HERE (label 0) — the ones we could save
df = spark.table(f"{CATALOG}.{SCHEMA}.account_features").toPandas()
live = df[df["is_churned"] == 0].copy()
feat_cols = ["plan_type", "industry", "country", "tenure_days", "monthly_recurring_revenue",
             "total_events_90d", "total_session_minutes_90d", "total_pages_viewed_90d"]

live["churn_risk"] = model.predict_proba(live[feat_cols])[:, 1].round(4)
live["risk_band"] = pd.cut(live["churn_risk"], bins=[-0.01, 0.4, 0.7, 1.01],
                           labels=["Low", "Medium", "High"])

scores = live[["account_id", "plan_type", "tenure_days", "monthly_recurring_revenue",
               "total_events_90d", "total_session_minutes_90d", "churn_risk", "risk_band"]] \
    .sort_values("churn_risk", ascending=False)

# 3) persist the risk list as a governed table
(spark.createDataFrame(scores.astype({"risk_band": "string"}))
      .write.mode("overwrite").option("overwriteSchema", "true")
      .saveAsTable(f"{CATALOG}.{SCHEMA}.account_churn_scores"))

print(f"Scored {len(scores)} active accounts -> {CATALOG}.{SCHEMA}.account_churn_scores")
print("Risk band counts:")
print(scores["risk_band"].value_counts().to_string())
print("\nTop 10 highest-risk accounts:")
print(scores.head(10).to_string(index=False))
