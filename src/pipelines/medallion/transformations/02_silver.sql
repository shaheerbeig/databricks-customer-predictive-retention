-- =====================================================================
-- SILVER LAYER
-- Join subscriptions -> accounts on account_id, add an is_churned flag,
-- and compute total usage per account over the last 90 days.
-- Materialized view: a join + per-account aggregation recomputed on refresh.
-- Expectations: account_id NOT NULL (FAIL), valid status (DROP), MRR >= 0 (WARN).
-- =====================================================================

CREATE OR REFRESH MATERIALIZED VIEW silver_account_health (
  account_id STRING COMMENT 'Primary key - account identifier',
  subscription_id STRING COMMENT 'Subscription identifier for the account',
  company_name STRING COMMENT 'Customer company name',
  industry STRING COMMENT 'Industry vertical',
  plan_type STRING COMMENT 'Plan tier: Starter / Growth / Enterprise',
  country STRING COMMENT 'Account billing country',
  contract_start_date DATE COMMENT 'Date the account contract started',
  monthly_recurring_revenue DOUBLE COMMENT 'Monthly recurring revenue (USD)',
  status STRING COMMENT 'Subscription status: active / churned / trial',
  renewal_date DATE COMMENT 'Contract renewal date',
  is_churned BOOLEAN COMMENT 'TRUE when the subscription status is churned',
  usage_event_count_90d BIGINT COMMENT 'Count of usage events in the last 90 days',
  total_session_minutes_90d DOUBLE COMMENT 'Total session minutes in the last 90 days',
  total_pages_viewed_90d BIGINT COMMENT 'Total pages viewed in the last 90 days',
  CONSTRAINT account_id_present EXPECT (account_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_status EXPECT (status IN ('active','churned','trial')) ON VIOLATION DROP ROW,
  CONSTRAINT non_negative_mrr EXPECT (monthly_recurring_revenue >= 0)
)
COMMENT 'Silver: account health combining subscription + account attributes with an is_churned flag and rolling 90-day usage totals per account.'
AS
WITH usage_90d AS (
  SELECT
    account_id,
    COUNT(*) AS usage_event_count_90d,
    SUM(session_duration_minutes) AS total_session_minutes_90d,
    SUM(pages_viewed) AS total_pages_viewed_90d
  FROM bronze_usage_events
  WHERE event_date >= date_sub(current_date(), 90)
  GROUP BY account_id
)
SELECT
  s.account_id,
  s.subscription_id,
  a.company_name,
  a.industry,
  a.plan_type,
  a.country,
  a.contract_start_date,
  s.monthly_recurring_revenue,
  s.status,
  s.renewal_date,
  (s.status = 'churned') AS is_churned,
  COALESCE(u.usage_event_count_90d, 0) AS usage_event_count_90d,
  COALESCE(u.total_session_minutes_90d, 0.0) AS total_session_minutes_90d,
  COALESCE(u.total_pages_viewed_90d, 0) AS total_pages_viewed_90d
FROM bronze_subscriptions s
JOIN bronze_accounts a ON s.account_id = a.account_id
LEFT JOIN usage_90d u ON s.account_id = u.account_id;

