# 05_ML_AI (Planned)

## Scope
Add native ML/AI for anomaly detection and advanced analytics directly in Snowflake.

## Candidate Snowflake features
- ML/AI functions (Anomaly Detection, Forecasting). Docs: https://docs.snowflake.com/en/user-guide/ml-functions
- Python Worksheets and Notebooks for model authoring. Docs: https://docs.snowflake.com/en/user-guide/ui-snowsight/notebooks
- Feature engineering with dbt in `ANALYTICS`. dbt Docs: https://docs.getdbt.com/

## Proposed next steps
- Baseline: call Anomaly Detection on recent `ANALYTICS.transaction_summary` windows.
- Visualization: expose anomaly flags in the Streamlit dashboard.
- Scheduling: evaluate with Tasks for periodic scoring.

## Considerations
- Cost visibility: serverless ML/AI consumption is itemized (see Service Consumption). Docs: https://docs.snowflake.com/en/user-guide/credits
- Governance: ensure masked PII persists throughout ML pipelines.
- Performance: keep input features minimal and filter early by time window.
