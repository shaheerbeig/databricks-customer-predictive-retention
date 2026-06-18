-- Databricks notebook source
-- Refreshes the plain serving snapshot the Streamlit app reads, from the gold MV.
CREATE OR REPLACE TABLE ai_dev_kit_testing.default.app_gold_mrr_by_plan_industry
COMMENT 'Plain serving snapshot of gold_mrr_by_plan_industry for the app.'
AS SELECT * FROM ai_dev_kit_testing.default.gold_mrr_by_plan_industry;
