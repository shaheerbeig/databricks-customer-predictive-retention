-- =====================================================================
-- BRONZE LAYER
-- Stream all three source Delta tables via STREAM(table) (CDF-enabled).
-- Critical key columns must be NOT NULL -> drop violating rows.
-- =====================================================================

CREATE OR REFRESH STREAMING TABLE bronze_accounts (
  account_idx BIGINT COMMENT 'Internal surrogate index from source generation',
  account_id STRING COMMENT 'Primary key - unique account identifier (ACC-#####)',
  company_name STRING COMMENT 'Customer company name',
  industry STRING COMMENT 'Industry vertical: Healthcare / Fintech / Retail / Logistics',
  plan_type STRING COMMENT 'Plan tier: Starter / Growth / Enterprise',
  country STRING COMMENT 'Account billing country',
  contract_start_date DATE COMMENT 'Date the account contract started',
  CONSTRAINT valid_account_id EXPECT (account_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Bronze: raw accounts streamed incrementally from the source Delta table; account_id key enforced not-null (rows dropped on violation).'
AS SELECT * FROM STREAM(ai_dev_kit_testing.default.accounts);


CREATE OR REFRESH STREAMING TABLE bronze_subscriptions (
  subscription_id STRING COMMENT 'Primary key - unique subscription identifier (SUB-#####)',
  account_id STRING COMMENT 'Foreign key to bronze_accounts.account_id',
  monthly_recurring_revenue DOUBLE COMMENT 'Monthly recurring revenue (USD)',
  status STRING COMMENT 'Subscription status: active / churned / trial',
  renewal_date DATE COMMENT 'Contract renewal date (past for churned, future for active/trial)',
  CONSTRAINT valid_subscription_id EXPECT (subscription_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Bronze: raw subscriptions streamed incrementally from the source Delta table; subscription_id key enforced not-null (rows dropped on violation).'
AS SELECT * FROM STREAM(ai_dev_kit_testing.default.subscriptions);


CREATE OR REFRESH STREAMING TABLE bronze_usage_events (
  event_id STRING COMMENT 'Primary key - unique usage event identifier (EVT-########)',
  account_id STRING COMMENT 'Foreign key to bronze_accounts.account_id',
  feature_name STRING COMMENT 'Product feature used in the event',
  event_date DATE COMMENT 'Date the usage event occurred',
  session_duration_minutes DOUBLE COMMENT 'Session length in minutes',
  pages_viewed INT COMMENT 'Number of pages viewed in the session',
  CONSTRAINT valid_event_id EXPECT (event_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Bronze: raw product usage events streamed incrementally from the source Delta table; event_id key enforced not-null (rows dropped on violation).'
AS SELECT * FROM STREAM(ai_dev_kit_testing.default.usage_events);
