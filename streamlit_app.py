import datetime
import os
import re
from typing import Any, Dict, Optional

import pandas as pd
import snowflake.connector
import streamlit as st


SNOWFLAKE_SECRET_KEY = "snowflake"
SNOWFLAKE_REQUIRED_FIELDS = [
    "user",
    "password",
    "account",
    "warehouse",
    "database",
    "schema",
]


def _translate_query_for_session(query: str) -> str:
    return re.sub(r"%\(([^)]+)\)s", r":\1", query)


class StreamlitSnowflakeCursor:
    def __init__(self, session: Any):
        self._session = session
        self._last_result = None

    def __enter__(self) -> "StreamlitSnowflakeCursor":
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        return None

    def execute(self, query: str, params: Optional[Dict[str, Any]] = None) -> "StreamlitSnowflakeCursor":
        translated_query = _translate_query_for_session(query)
        if params:
            self._last_result = self._session.sql(translated_query, **params)
        else:
            self._last_result = self._session.sql(translated_query)
        return self

    def fetch_pandas_all(self) -> pd.DataFrame:
        if self._last_result is None:
            return pd.DataFrame()
        return self._last_result.to_pandas()


class StreamlitSnowflakeConnection:
    def __init__(self, session: Any):
        self._session = session

    def cursor(self) -> StreamlitSnowflakeCursor:
        return StreamlitSnowflakeCursor(self._session)

    def session(self) -> Any:
        return self._session


def get_snowflake_credentials() -> Dict[str, Any]:
    if SNOWFLAKE_SECRET_KEY in st.secrets:
        credentials = st.secrets[SNOWFLAKE_SECRET_KEY]
        source = "Streamlit secrets"
    else:
        credentials = {
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
        }
        source = "environment variables"

    missing_fields = [
        field for field in SNOWFLAKE_REQUIRED_FIELDS if not credentials.get(field)
    ]

    if missing_fields:
        st.error(
            "Snowflake credentials are missing. "
            "Set all required values in Streamlit secrets or environment variables."
        )
        st.error(
            f"Missing fields: {', '.join(missing_fields)}. "
            f"Configured via {source}."
        )
        st.markdown(
            "**Local development:** create `.streamlit/secrets.toml` with a `[snowflake]` group. "
            "**Deployment:** add the same `snowflake` secret group in Streamlit app settings or set the `SNOWFLAKE_*` environment variables."
        )
        st.stop()

    return credentials


def get_snowflake_connection():
    try:
        cnx = st.connection("snowflake")
        if hasattr(cnx, "cursor"):
            return cnx
        if hasattr(cnx, "session"):
            return StreamlitSnowflakeConnection(cnx.session())
    except Exception:
        pass

    creds = get_snowflake_credentials()

    return snowflake.connector.connect(
        user=creds["user"],
        password=creds["password"],
        account=creds["account"],
        warehouse=creds["warehouse"],
        database=creds["database"],
        schema=creds["schema"],
    )


@st.cache_resource
def get_connection():
    return get_snowflake_connection()


@st.cache_data(ttl=300)
def load_accounts() -> pd.DataFrame:
    query = """
    SELECT id, name, account_type, currency, balance, created_at
    FROM accounts
    ORDER BY name
    """
    with get_connection().cursor() as cur:
        cur.execute(query)
        return cur.fetch_pandas_all()


@st.cache_data(ttl=300)
def load_transactions(account_id: Optional[int] = None) -> pd.DataFrame:
    conditions = []
    params = {}
    if account_id is not None:
        conditions.append("t.account_id = %(account_id)s")
        params["account_id"] = account_id

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    query = f"""
    SELECT
        t.id,
        t.posted_at,
        t.description,
        t.category,
        t.amount,
        t.currency,
        a.name AS account_name,
        t.created_at
    FROM transactions t
    LEFT JOIN accounts a ON t.account_id = a.id
    {where_clause}
    ORDER BY t.posted_at DESC
    """
    with get_connection().cursor() as cur:
        cur.execute(query, params)
        return cur.fetch_pandas_all()


@st.cache_data(ttl=300)
def load_budgets() -> pd.DataFrame:
    query = """
    SELECT id, category, amount, period, created_at
    FROM budgets
    ORDER BY category
    """
    with get_connection().cursor() as cur:
        cur.execute(query)
        return cur.fetch_pandas_all()


def initialize_schema():
    commands = [
        "CREATE TABLE IF NOT EXISTS accounts (id INTEGER AUTOINCREMENT PRIMARY KEY, name STRING, account_type STRING, currency STRING, balance FLOAT, created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP())",
        "CREATE TABLE IF NOT EXISTS transactions (id INTEGER AUTOINCREMENT PRIMARY KEY, posted_at DATE, description STRING, category STRING, amount FLOAT, currency STRING, account_id INTEGER, created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP())",
        "CREATE TABLE IF NOT EXISTS budgets (id INTEGER AUTOINCREMENT PRIMARY KEY, category STRING, amount FLOAT, period STRING, created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP())",
    ]
    with get_connection().cursor() as cur:
        for command in commands:
            cur.execute(command)


def add_account(name: str, account_type: str, currency: str, balance: float):
    query = """
    INSERT INTO accounts (name, account_type, currency, balance)
    VALUES (%(name)s, %(account_type)s, %(currency)s, %(balance)s)
    """
    with get_connection().cursor() as cur:
        cur.execute(
            query,
            {
                "name": name,
                "account_type": account_type,
                "currency": currency,
                "balance": balance,
            },
        )
    st.cache_data.clear()
    st.success(f"Account '{name}' added.")


def add_transaction(
    posted_at: datetime.date,
    description: str,
    category: str,
    amount: float,
    currency: str,
    account_id: int,
):
    query = """
    INSERT INTO transactions (posted_at, description, category, amount, currency, account_id)
    VALUES (%(posted_at)s, %(description)s, %(category)s, %(amount)s, %(currency)s, %(account_id)s)
    """
    with get_connection().cursor() as cur:
        cur.execute(
            query,
            {
                "posted_at": posted_at,
                "description": description,
                "category": category,
                "amount": amount,
                "currency": currency,
                "account_id": account_id,
            },
        )
    st.cache_data.clear()
    st.success("Transaction added.")


def add_budget(category: str, amount: float, period: str):
    query = """
    INSERT INTO budgets (category, amount, period)
    VALUES (%(category)s, %(amount)s, %(period)s)
    """
    with get_connection().cursor() as cur:
        cur.execute(
            query,
            {"category": category, "amount": amount, "period": period},
        )
    st.cache_data.clear()
    st.success("Budget saved.")


def format_money(value: float, currency: str = "USD") -> str:
    return f"{currency} {value:,.2f}"


def show_dashboard():
    st.header("Family Finance Dashboard")
    transactions = load_transactions()
    accounts = load_accounts()

    total_balance = accounts["balance"].sum() if not accounts.empty else 0.0
    total_spend = transactions.loc[transactions["amount"] < 0, "amount"].sum() if not transactions.empty else 0.0
    total_income = transactions.loc[transactions["amount"] > 0, "amount"].sum() if not transactions.empty else 0.0

    col1, col2, col3 = st.columns(3)
    col1.metric("Total balance", format_money(total_balance))
    col2.metric("Total income", format_money(total_income))
    col3.metric("Total spending", format_money(total_spend))

    if not transactions.empty:
        category_summary = (
            transactions.groupby("category")["amount"].sum().reset_index().sort_values("amount")
        )
        st.subheader("Spending by category")
        st.bar_chart(category_summary.set_index("category"))

        monthly = (
            transactions.assign(posted_at=pd.to_datetime(transactions["posted_at"]))
            .groupby(pd.Grouper(key="posted_at", freq="M"))["amount"]
            .sum()
            .reset_index()
        )
        st.subheader("Monthly cash flow")
        st.line_chart(monthly.set_index("posted_at"))

    st.subheader("Accounts")
    st.dataframe(accounts)


def show_transactions_page():
    st.header("Transactions")
    accounts = load_accounts()
    account_options = {row["name"]: int(row["ID"]) for _, row in accounts.iterrows()} if not accounts.empty else {}

    with st.expander("Add new transaction"):
        if accounts.empty:
            st.warning("Create an account before adding transactions.")
        else:
            posted_at = st.date_input("Date", datetime.date.today())
            description = st.text_input("Description")
            category = st.text_input("Category", value="Groceries")
            amount = st.number_input("Amount", value=0.0, format="%f")
            currency = st.selectbox("Currency", options=sorted(accounts["currency"].unique()))
            account_name = st.selectbox("Account", options=list(account_options.keys()))
            if st.button("Save transaction"):
                add_transaction(
                    posted_at,
                    description,
                    category,
                    amount,
                    currency,
                    account_options[account_name],
                )

    selected_account = st.selectbox("Filter by account", options=["All"] + list(account_options.keys()))
    account_id = account_options[selected_account] if selected_account != "All" else None
    transactions = load_transactions(account_id)
    st.dataframe(transactions)


def show_accounts_page():
    st.header("Accounts")
    accounts = load_accounts()

    if accounts.empty:
        st.info("No accounts found. Add a new account below to start tracking balances.")
    else:
        total_balance_by_currency = (
            accounts.groupby("CURRENCY")["BALANCE"].sum().reset_index()
        )
        overall_balance = accounts["BALANCE"].sum()

        st.metric("Total balance", format_money(overall_balance))
        st.subheader("Balance by currency")
        st.dataframe(total_balance_by_currency.rename(columns={"CURRENCY": "Currency", "BALANCE": "Total balance"}))

        st.subheader("Account details")
        st.dataframe(
            accounts[["ID", "NAME", "ACCOUNT_TYPE", "CURRENCY", "BALANCE"]]
            .rename(columns={
                "ID": "Account ID",
                "NAME": "Name",
                "ACCOUNT_TYPE": "Type",
                "CURRENCY": "Currency",
                "BALANCE": "Balance",
            })
        )

    with st.expander("Add account"):
        name = st.text_input("Account name")
        account_type = st.selectbox("Type", ["Savings", "Checking", "Credit", "Cash"])
        currency = st.text_input("Currency", value="USD")
        balance = st.number_input("Starting balance", value=0.0, format="%f")
        if st.button("Create account"):
            if not name:
                st.warning("Account name is required.")
            else:
                add_account(name, account_type, currency, balance)


def show_budgets_page():
    st.header("Budgets")
    budgets = load_budgets()
    st.dataframe(budgets)

    with st.expander("Add budget"):
        category = st.text_input("Category", value="Groceries")
        amount = st.number_input("Budget amount", value=0.0, format="%f")
        period = st.selectbox("Period", ["Monthly", "Weekly", "Yearly"])
        if st.button("Save budget"):
            add_budget(category, amount, period)


def main():
    st.set_page_config(page_title="Family Finance", page_icon="💰", layout="wide")
    st.title("Family Finance Manager")

    initialize_schema()

    page = st.sidebar.selectbox("Choose page", ["Dashboard", "Transactions", "Accounts", "Budgets"])
    if page == "Dashboard":
        show_dashboard()
    elif page == "Transactions":
        show_transactions_page()
    elif page == "Accounts":
        show_accounts_page()
    elif page == "Budgets":
        show_budgets_page()


if __name__ == "__main__":
    main()
