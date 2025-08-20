# 04_Visualization

## Scope
Deliver interactive analytics with a native Streamlit app in Snowflake.

## Snowflake features used
- Streamlit in Snowflake. Docs: https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit
- SnowCLI for deployment. Docs: https://docs.snowflake.com/en/developer-guide/streamlit/create-streamlit-snowflake-cli
- get_active_session() for in-session queries. Docs: https://docs.snowflake.com/en/developer-guide/streamlit/session-objects

## What is implemented in this repo
- `streamlit_app/` project configured via `snowflake.yml` to deploy into `FSI_DEMO.ANALYTICS`.
- `streamlit_app.py` with simplified filters (time period, transaction types), core KPIs, and PII masking demo.
- Dependencies pinned in `environment.yml` with Snowflake-approved channels.

## How to deploy
```bash
cd streamlit_app
snow streamlit deploy --replace
# App: FSI_DEMO.ANALYTICS.FSI_ANALYTICS_DASHBOARD
```

## Benefits
- No egress; app runs next to the data.
- Honors masking policies automatically.
- Minimal ops with SnowCLI and Workspaces.

## Considerations
- Do not set owner to a role that should bypass masking unless intentionally demonstrating steward access.
- Keep dependencies minimal and use supported Anaconda channels only.
- Avoid USE ROLE in the app; roles are set at connection level.
