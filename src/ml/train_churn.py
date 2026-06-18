"""Train churn models with full MLflow lifecycle, logging to the Databricks
workspace and registering the winner to Unity Catalog. Runs locally via
Databricks Connect (data pulled from UC; sklearn training runs on this machine).
"""
import mlflow, mlflow.sklearn
from mlflow.models import infer_signature
import pandas as pd
from databricks.connect import DatabricksSession
from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.inspection import permutation_importance
from sklearn.metrics import roc_auc_score, accuracy_score, precision_score, recall_score, f1_score

CATALOG, SCHEMA = "ai_dev_kit_testing", "default"
spark = DatabricksSession.builder.serverless(True).getOrCreate()

mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment("/Users/mmtahashafiq@gmail.com/saas_churn")

# ---- load + 80/20 STRATIFIED split ----
df = spark.table(f"{CATALOG}.{SCHEMA}.account_features").toPandas()
y = df["is_churned"].astype(int)
X = df.drop(columns=["account_id", "is_churned"])
cat = ["plan_type", "industry", "country"]
num = ["tenure_days", "monthly_recurring_revenue", "total_events_90d", "total_session_minutes_90d", "total_pages_viewed_90d"]
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, stratify=y, random_state=42)
print(f"train={len(X_train)}  test={len(X_test)}  churn_rate_train={y_train.mean():.3f}  churn_rate_test={y_test.mean():.3f}")

def pre(scale):
    return ColumnTransformer([("cat", OneHotEncoder(handle_unknown="ignore"), cat),
                              ("num", StandardScaler() if scale else "passthrough", num)])

models = {
    "logreg_baseline": Pipeline([("pre", pre(True)), ("clf", LogisticRegression(max_iter=1000))]),
    "gradient_boosted_trees": Pipeline([("pre", pre(False)), ("clf", HistGradientBoostingClassifier(random_state=42))]),
}

results, best = [], None
for name, pipe in models.items():
    with mlflow.start_run(run_name=name) as run:
        pipe.fit(X_train, y_train)
        proba = pipe.predict_proba(X_test)[:, 1]
        pred = (proba >= 0.5).astype(int)
        m = {"auc": float(roc_auc_score(y_test, proba)),
             "accuracy": float(accuracy_score(y_test, pred)),
             "precision": float(precision_score(y_test, pred)),
             "recall": float(recall_score(y_test, pred)),
             "f1": float(f1_score(y_test, pred))}
        mlflow.log_params({"model_type": name, "n_train": len(X_train), "n_test": len(X_test)})
        mlflow.log_metrics(m)
        mlflow.sklearn.log_model(pipe, artifact_path="model",
                                 signature=infer_signature(X_test, pred), input_example=X_test.head(3))
        results.append({"model": name, "run_id": run.info.run_id, **{k: round(v, 4) for k, v in m.items()}})
        print(f"  {name}: AUC={m['auc']:.4f} acc={m['accuracy']:.4f} prec={m['precision']:.4f} rec={m['recall']:.4f} f1={m['f1']:.4f}")
        if best is None or m["auc"] > best["auc"]:
            best = {"model": name, "run_id": run.info.run_id, "auc": m["auc"], "pipe": pipe}

# ---- register the winner to Unity Catalog ----
reg_name = f"{CATALOG}.{SCHEMA}.account_churn_model"
mv = mlflow.register_model(f"runs:/{best['run_id']}/model", reg_name)
print(f"REGISTERED {reg_name} version {mv.version}  (winner: {best['model']}, AUC={best['auc']:.4f})")

# ---- which features mattered (permutation importance on the winner) ----
perm = permutation_importance(best["pipe"], X_test, y_test, n_repeats=5, random_state=42, scoring="roc_auc")
imp = sorted([(c, float(round(v, 4))) for c, v in zip(X.columns, perm.importances_mean)], key=lambda t: -t[1])
print("FEATURE IMPORTANCE (AUC drop when shuffled):")
for f, v in imp:
    print(f"  {f}: {v}")

# ---- persist results as small tables ----
spark.createDataFrame(pd.DataFrame(results)).write.mode("overwrite").option("overwriteSchema","true").saveAsTable(f"{CATALOG}.{SCHEMA}.churn_model_metrics")
spark.createDataFrame(pd.DataFrame(imp, columns=["feature", "importance"])).write.mode("overwrite").option("overwriteSchema","true").saveAsTable(f"{CATALOG}.{SCHEMA}.churn_feature_importance")
spark.createDataFrame(pd.DataFrame([{"best_model": best["model"], "best_auc": round(best["auc"],4),
    "registered_model": reg_name, "version": int(mv.version), "n_train": len(X_train), "n_test": len(X_test)}])
).write.mode("overwrite").option("overwriteSchema","true").saveAsTable(f"{CATALOG}.{SCHEMA}.churn_training_status")
print("DONE")
