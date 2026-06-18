# Databricks notebook source
# Appends a MESSY batch to the source tables every run, with deliberate
# data-quality violations so the pipeline's EXPECT constraints are exercised:
#   - ~5% NULL account_id / subscription_id / event_id  -> DROPPED at bronze
#   - ~10% invalid status ('paused')                     -> DROPPED at silver
#   - ~8% negative monthly_recurring_revenue             -> WARNED at silver
# IDs are derived from current MAX so every run is unique (safe to append).
# Pure Spark (no external deps) so it runs on serverless without pip installs.
from pyspark.sql import functions as F

C = "ai_dev_kit_testing.default"
N_ACC = 2000
N_EVENTS = 5000

base_acc = spark.sql(f"SELECT COALESCE(MAX(account_idx), -1) + 1 AS b FROM {C}.accounts").collect()[0]["b"]
base_evt = spark.sql(
    f"SELECT COALESCE(MAX(CAST(substr(event_id,5) AS BIGINT)), -1) + 1 AS b "
    f"FROM {C}.usage_events WHERE event_id IS NOT NULL"
).collect()[0]["b"]
print(f"base_acc={base_acc}  base_evt={base_evt}")

# COMMAND ----------
# accounts: ~5% NULL account_id (dropped at bronze)
acc = (
    spark.range(0, N_ACC)
    .withColumn("account_idx", F.col("id") + F.lit(base_acc))
    .withColumn("_rn", F.rand())
    .withColumn("account_id",
        F.when(F.col("_rn") < 0.05, F.lit(None).cast("string"))
         .otherwise(F.concat(F.lit("ACC-"), F.lpad(F.col("account_idx").cast("string"), 5, "0"))))
    .withColumn("company_name", F.concat(F.lit("Company "), F.col("account_idx").cast("string")))
    .withColumn("_ri", F.rand())
    .withColumn("industry",
        F.when(F.col("_ri") < 0.30, "Fintech").when(F.col("_ri") < 0.58, "Healthcare")
         .when(F.col("_ri") < 0.80, "Retail").otherwise("Logistics"))
    .withColumn("_rp", F.rand())
    .withColumn("plan_type",
        F.when(F.col("_rp") < 0.60, "Starter").when(F.col("_rp") < 0.90, "Growth").otherwise("Enterprise"))
    .withColumn("_rc", F.rand())
    .withColumn("country",
        F.when(F.col("_rc") < 0.45, "United States").when(F.col("_rc") < 0.7, "United Kingdom")
         .when(F.col("_rc") < 0.85, "Germany").otherwise("India"))
    .withColumn("contract_start_date", F.date_sub(F.current_date(), (F.rand() * 700 + 30).cast("int")))
    .select("account_idx", "account_id", "company_name", "industry", "plan_type", "country", "contract_start_date")
)
acc.write.mode("append").saveAsTable(f"{C}.accounts")
print("appended accounts:", N_ACC)

# COMMAND ----------
# subscriptions for the new (valid) accounts: ~5% NULL subscription_id (drop@bronze),
# ~10% invalid status 'paused' (drop@silver), ~8% negative MRR (warn@silver)
new_acc = spark.table(f"{C}.accounts").filter(
    (F.col("account_idx") >= base_acc) & F.col("account_id").isNotNull()
)
sub = (
    new_acc.select("account_idx", "account_id", "plan_type")
    .withColumn("_rs", F.rand())
    .withColumn("subscription_id",
        F.when(F.col("_rs") < 0.05, F.lit(None).cast("string"))
         .otherwise(F.concat(F.lit("SUB-"), F.lpad(F.col("account_idx").cast("string"), 5, "0"))))
    .withColumn("_rst", F.rand())
    .withColumn("status",
        F.when(F.col("_rst") < 0.10, "paused")          # invalid -> drop at silver
         .when(F.col("_rst") < 0.45, "churned")
         .when(F.col("_rst") < 0.85, "active").otherwise("trial"))
    .withColumn("_base_mrr",
        F.when(F.col("plan_type") == "Enterprise", F.rand() * 8000 + 3000)
         .when(F.col("plan_type") == "Growth", F.rand() * 1500 + 500).otherwise(F.rand() * 200 + 40))
    .withColumn("_rm", F.rand())
    .withColumn("monthly_recurring_revenue",
        F.round(F.when(F.col("_rm") < 0.08, F.col("_base_mrr") * F.lit(-1.0)).otherwise(F.col("_base_mrr")), 2))
    .withColumn("renewal_date", F.date_add(F.current_date(), (F.rand() * 365 - 60).cast("int")))
    .select("subscription_id", "account_id", "monthly_recurring_revenue", "status", "renewal_date")
)
sub.write.mode("append").saveAsTable(f"{C}.subscriptions")
print("appended subscriptions")

# COMMAND ----------
# usage events: ~5% NULL event_id (drop at bronze)
ev = (
    spark.range(0, N_EVENTS)
    .withColumn("evt_idx", F.col("id") + F.lit(base_evt))
    .withColumn("_re", F.rand())
    .withColumn("event_id",
        F.when(F.col("_re") < 0.05, F.lit(None).cast("string"))
         .otherwise(F.concat(F.lit("EVT-"), F.lpad(F.col("evt_idx").cast("string"), 8, "0"))))
    .withColumn("_pick", (F.rand() * N_ACC).cast("long") + F.lit(base_acc))
    .withColumn("account_id", F.concat(F.lit("ACC-"), F.lpad(F.col("_pick").cast("string"), 5, "0")))
    .withColumn("_rf", F.rand())
    .withColumn("feature_name",
        F.when(F.col("_rf") < 0.30, "Dashboard").when(F.col("_rf") < 0.55, "Reports")
         .when(F.col("_rf") < 0.72, "Search").when(F.col("_rf") < 0.85, "Integrations")
         .when(F.col("_rf") < 0.94, "API").otherwise("Export"))
    .withColumn("event_date", F.date_sub(F.current_date(), (F.rand() * 90).cast("int")))
    .withColumn("session_duration_minutes", F.round(F.rand() * 60 + 1, 1))
    .withColumn("pages_viewed", (F.rand() * 40 + 1).cast("int"))
    .select("event_id", "account_id", "feature_name", "event_date", "session_duration_minutes", "pages_viewed")
)
ev.write.mode("append").saveAsTable(f"{C}.usage_events")
print("appended usage_events:", N_EVENTS)
print("DONE")
