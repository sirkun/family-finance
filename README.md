# Family Finance

A Streamlit app for managing your family finances with Snowflake as the data backend.

## What is included

- `streamlit_app.py` — Streamlit application for accounts, transactions, budgets, and dashboard views
- `requirements.txt` — dependencies for Streamlit, Snowflake connector, and pandas

## Features

- Create and manage accounts
- Add and review transactions
- Track budgets by category and period
- Dashboard summaries for balance, spending, and cash flow
- Stores all data in Snowflake

## Local development

1. Install dependencies:

```bash
python -m pip install -r requirements.txt
```

2. Create a Streamlit secrets file for local development:

- Create a folder named `.streamlit`
- Create `.streamlit/secrets.toml`

Example `.streamlit/secrets.toml`:

```toml
[snowflake]
user = "YOUR_SNOWFLAKE_USER"
password = "YOUR_SNOWFLAKE_PASSWORD"
account = "YOUR_SNOWFLAKE_ACCOUNT"
warehouse = "YOUR_SNOWFLAKE_WAREHOUSE"
database = "YOUR_DATABASE"
schema = "PUBLIC"
```

3. Run the app:

```bash
streamlit run streamlit_app.py
```

## Streamlit Cloud / deployment

For deployment on Streamlit Cloud, add the same `snowflake` secret keys in the app settings. The repo can be launched directly from `streamlit_app.py`.

## Snowflake schema

The app initializes the following tables automatically if they are not present:

- `accounts`
- `transactions`
- `budgets`

If you need manual schema setup, create these tables in your Snowflake database.

## Next steps

- Add category-level budgeting and alerts
- Create family member profiles and permissions
- Add CSV import/export for transactions
- Add charts for savings, debt, and recurring expenses
