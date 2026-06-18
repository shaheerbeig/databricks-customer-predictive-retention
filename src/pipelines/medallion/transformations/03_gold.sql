-- =====================================================================
-- GOLD LAYER
-- Aggregate monthly recurring revenue by plan_type and industry.
-- Expectations: active_mrr >= 0 (WARN), account_count > 0 (FAIL).
-- =====================================================================

CREATE OR REFRESH MATERIALIZED VIEW gold_mrr_by_plan_industry (
  plan_type STRING COMMENT 'Plan tier: Starter / Growth / Enterprise',
  industry STRING COMMENT 'Industry vertical: Healthcare / Fintech / Retail / Logistics',
  active_mrr DOUBLE COMMENT 'Sum of monthly recurring revenue for active subscriptions (USD)',
  total_mrr DOUBLE COMMENT 'Sum of monthly recurring revenue across all statuses (USD)',
  account_count BIGINT COMMENT 'Distinct number of accounts in the plan/industry segment',
  churned_account_count BIGINT COMMENT 'Number of churned accounts in the segment',
  CONSTRAINT non_negative_active_mrr EXPECT (active_mrr >= 0),
  CONSTRAINT positive_account_count EXPECT (account_count > 0) ON VIOLATION FAIL UPDATE
)
COMMENT 'Gold: monthly recurring revenue and account counts aggregated by plan_type and industry for executive reporting.'
AS
SELECT
  plan_type,
  industry,
  SUM(CASE WHEN status = 'active' THEN monthly_recurring_revenue ELSE 0 END) AS active_mrr,
  SUM(monthly_recurring_revenue) AS total_mrr,
  COUNT(DISTINCT account_id) AS account_count,
  SUM(CASE WHEN is_churned THEN 1 ELSE 0 END) AS churned_account_count
FROM silver_account_health
GROUP BY plan_type, industry;
