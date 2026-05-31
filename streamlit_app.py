import datetime
import json
import os
import re
import urllib.parse
import urllib.request
from typing import Any, Dict, Optional, Callable

import pandas as pd
import snowflake.connector
import streamlit as st

SNOWFLAKE_REQUIRED_FIELDS = [
    "user",
    "password",
    "account",
    "warehouse",
    "database",
    "schema",
]

CONFIG_FILE = "config.json"
MONEY_API_TOKEN_KEY = "money_api_token"
MONEY_API_BASE = "https://money.quhou123.com/Api"
MONEY_API_ENDPOINTS = {
    "accounts": f"{MONEY_API_BASE}/getAccounts",
    "transactions": f"{MONEY_API_BASE}/getTransactions",
}

# Transaction type constants
TRANSACTION_TYPE_INCOME = 1
TRANSACTION_TYPE_EXPENSE = 2
TRANSACTION_TYPE_TRANSFER = 3


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
    credentials = {
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "authenticator": os.getenv("SNOWFLAKE_AUTHENTICATOR"),
    }

    missing_fields = [
        field for field in SNOWFLAKE_REQUIRED_FIELDS if not credentials.get(field)
    ]

    if missing_fields:
        st.error(
            "Snowflake credentials are missing. "
            "Set the required `SNOWFLAKE_*` environment variables."
        )
        st.error(
            f"Missing fields: {', '.join(missing_fields)}."
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

    connection_kwargs = {
        "user": creds["user"],
        "password": creds["password"],
        "account": creds["account"],
        "warehouse": creds["warehouse"],
        "database": creds["database"],
        "schema": creds["schema"],
    }

    if creds.get("role"):
        connection_kwargs["role"] = creds["role"]
    if creds.get("authenticator"):
        connection_kwargs["authenticator"] = creds["authenticator"]

    return snowflake.connector.connect(**connection_kwargs)


@st.cache_resource
def get_connection():
    return get_snowflake_connection()


def load_config() -> Dict[str, Any]:
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as config_file:
                return json.load(config_file)
        except Exception as exc:
            st.warning(f"Unable to read {CONFIG_FILE}: {exc}")
    return {}


def get_money_api_token() -> str:
    config = load_config()
    token = config.get(MONEY_API_TOKEN_KEY) or os.getenv("MONEY_API_TOKEN")
    if not token:
        st.error(
            "Money API token is missing. "
            "Add `money_api_token` to config.json or set the MONEY_API_TOKEN environment variable."
        )
        st.stop()
    return token


def _api_post_form(url: str, data: Dict[str, Any]) -> Any:
    encoded = urllib.parse.urlencode(data).encode("utf-8")
    request = urllib.request.Request(url, data=encoded, method="POST")
    request.add_header("Content-Type", "application/x-www-form-urlencoded")

    with urllib.request.urlopen(request, timeout=30) as response:
        body = response.read().decode("utf-8")

    payload = json.loads(body)
    if isinstance(payload, dict) and payload.get("status") != 1:
        raise RuntimeError(payload.get("msg", "Money API request failed"))

    if isinstance(payload, dict):
        for key in ("data", "result", "list", "items", "records"):
            if key in payload:
                return payload[key]
    return payload


_PYFORMAT_PARAM_RE = re.compile(r"%\(([^)]+)\)s")


def _api_get_value(row: Dict[str, Any], keys: tuple, default: Any = None) -> Any:
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
    return default


def _convert_pyformat_dict_to_qmarks(query: str, params: Dict[str, Any]) -> tuple[str, list[Any]]:
    param_order: list[str] = []

    def _replace(match: re.Match[str]) -> str:
        param_order.append(match.group(1))
        return "?"

    converted_query = _PYFORMAT_PARAM_RE.sub(_replace, query)
    converted_params = [params[name] for name in param_order]
    return converted_query, converted_params


def execute_query(cur: Any, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
    if params is None:
        return cur.execute(query)

    try:
        return cur.execute(query, params)
    except snowflake.connector.errors.ProgrammingError as exc:
        if "Binding parameters must be a list" in str(exc) and isinstance(params, dict):
            converted_query, converted_params = _convert_pyformat_dict_to_qmarks(query, params)
            return cur.execute(converted_query, converted_params)
        raise


def _api_get_date(row: Dict[str, Any], keys: tuple) -> Optional[datetime.date]:
    for key in keys:
        value = row.get(key)
        if value in (None, ""):
            continue
        try:
            if isinstance(value, str) and value.isdigit():
                ts = int(value) / 1000
                return datetime.datetime.utcfromtimestamp(ts).date()
            if isinstance(value, (int, float)):
                ts = int(value) / 1000
                return datetime.datetime.utcfromtimestamp(ts).date()
            return datetime.date.fromisoformat(value)
        except Exception:
            continue
    return None


def _table_exists(table_name: str) -> bool:
    query = """
    SELECT COUNT(*) AS count
    FROM information_schema.tables
    WHERE table_schema = CURRENT_SCHEMA()
      AND table_name = %(table_name)s
    """
    with get_connection().cursor() as cur:
        execute_query(cur, query, {"table_name": table_name.upper()})
        return int(cur.fetch_pandas_all()["COUNT"].iloc[0]) > 0


def _get_column_type(table_name: str, column_name: str) -> Optional[str]:
    query = """
    SELECT data_type
    FROM information_schema.columns
    WHERE table_schema = CURRENT_SCHEMA()
      AND table_name = %(table_name)s
      AND column_name = %(column_name)s
    """
    with get_connection().cursor() as cur:
        execute_query(cur, query, {"table_name": table_name.upper(), "column_name": column_name.upper()})
        df = cur.fetch_pandas_all()
        if df.empty:
            return None
        return df["DATA_TYPE"].iloc[0].upper()


def _ensure_schema_migrations_table():
    with get_connection().cursor() as cur:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations (name STRING PRIMARY KEY, applied_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP())"
        )


def run_migration_once(name: str, fn: Callable[[Any], None]) -> bool:
    """Run a migration function once and record it in `schema_migrations`.

    Returns True if migration ran, False if it was already applied.
    """
    _ensure_schema_migrations_table()
    with get_connection().cursor() as cur:
        execute_query(cur, "SELECT 1 FROM schema_migrations WHERE name = %(name)s", {"name": name})
        if not cur.fetch_pandas_all().empty:
            return False
        # run migration function with the cursor
        fn(cur)
        execute_query(cur, "INSERT INTO schema_migrations (name) VALUES (%(name)s)", {"name": name})
        return True


def _migrate_table_to_string_id(table_name: str, create_sql: str, copy_sql: str) -> None:
    temp_table = f"{table_name}_NEW"
    with get_connection().cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {temp_table}")
        cur.execute(create_sql)
        execute_query(cur, copy_sql)
        cur.execute(f"ALTER TABLE {table_name} RENAME TO {table_name}_OLD")
        cur.execute(f"ALTER TABLE {temp_table} RENAME TO {table_name}")
        cur.execute(f"DROP TABLE IF EXISTS {table_name}_OLD")


def _ensure_accounts_table():
    if not _table_exists("ACCOUNTS"):
        execute_query(
            get_connection().cursor(),
            "CREATE TABLE IF NOT EXISTS accounts (id STRING PRIMARY KEY, name STRING, account_type STRING, currency STRING, balance FLOAT, created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP())",
        )
        return

    current_type = _get_column_type("ACCOUNTS", "ID")
    if current_type in ("STRING", "TEXT", "VARCHAR", "CHAR"):
        return

    _migrate_table_to_string_id(
        "ACCOUNTS",
        "CREATE TABLE accounts_new (id STRING PRIMARY KEY, name STRING, account_type STRING, currency STRING, balance FLOAT, created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP())",
        "INSERT INTO accounts_new (id, name, account_type, currency, balance, created_at) SELECT TO_VARCHAR(id), name, account_type, currency, TRY_TO_DOUBLE(balance), created_at FROM accounts",
    )


def _ensure_transactions_table():
    if not _table_exists("TRANSACTIONS"):
        execute_query(
            get_connection().cursor(),
            "CREATE TABLE IF NOT EXISTS transactions (id STRING PRIMARY KEY, posted_at DATE, description STRING, category STRING, amount FLOAT, currency STRING, account_id STRING, from_account_id STRING, to_account_id STRING, transaction_type INTEGER, created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP())",
        )
        return

    id_type = _get_column_type("TRANSACTIONS", "ID")
    account_id_type = _get_column_type("TRANSACTIONS", "ACCOUNT_ID")
    if id_type in ("STRING", "TEXT", "VARCHAR", "CHAR") and account_id_type in ("STRING", "TEXT", "VARCHAR", "CHAR"):
        def _add_transfer_columns(cur: Any):
            # Add columns if they don't exist to avoid SELECT compilation errors
            if _get_column_type("TRANSACTIONS", "FROM_ACCOUNT_ID") is None:
                cur.execute("ALTER TABLE transactions ADD COLUMN from_account_id STRING")
            if _get_column_type("TRANSACTIONS", "TO_ACCOUNT_ID") is None:
                cur.execute("ALTER TABLE transactions ADD COLUMN to_account_id STRING")
            if _get_column_type("TRANSACTIONS", "TRANSACTION_TYPE") is None:
                cur.execute("ALTER TABLE transactions ADD COLUMN transaction_type INTEGER")

        run_migration_once("add_transactions_transfer_columns", _add_transfer_columns)
        return

    _migrate_table_to_string_id(
        "TRANSACTIONS",
        "CREATE TABLE transactions_new (id STRING PRIMARY KEY, posted_at DATE, description STRING, category STRING, amount FLOAT, currency STRING, account_id STRING, from_account_id STRING, to_account_id STRING, transaction_type INTEGER, created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP())",
        "INSERT INTO transactions_new (id, posted_at, description, category, amount, currency, account_id, from_account_id, to_account_id, transaction_type, created_at) SELECT TO_VARCHAR(id), posted_at, description, category, TRY_TO_DOUBLE(amount), currency, TO_VARCHAR(account_id), TO_VARCHAR(NULL), TO_VARCHAR(NULL), NULL, created_at FROM transactions",
    )


def initialize_schema():
    _ensure_accounts_table()
    _ensure_transactions_table()
    with get_connection().cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS budgets (id INTEGER AUTOINCREMENT PRIMARY KEY, category STRING, amount FLOAT, period STRING, created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP())")
        cur.execute("CREATE TABLE IF NOT EXISTS sync_state (dataset STRING PRIMARY KEY, last_synced_at TIMESTAMP_LTZ, last_synced_id STRING, row_count NUMBER, updated_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP())")


def fetch_api_accounts() -> list[Dict[str, Any]]:
    raw = _api_post_form(MONEY_API_ENDPOINTS["accounts"], {"token": get_money_api_token()})
    if isinstance(raw, dict):
        return raw.get("accounts") or raw.get("data") or raw.get("list") or []
    return raw if isinstance(raw, list) else []


def fetch_api_transactions() -> list[Dict[str, Any]]:
    raw = _api_post_form(MONEY_API_ENDPOINTS["transactions"], {"token": get_money_api_token()})
    if isinstance(raw, dict):
        return raw.get("transactions") or raw.get("data") or raw.get("list") or []
    return raw if isinstance(raw, list) else []


def sync_account_row(row: Dict[str, Any]) -> None:
    account_id = _api_get_value(row, ("id", "account_id", "accountId"))
    if account_id is None:
        return

    params = {
        "id": account_id,
        "name": _api_get_value(row, ("name", "account_name", "accountName"), ""),
        "account_type": _api_get_value(row, ("type", "account_type", "accountType"), ""),
        "currency": _api_get_value(row, ("currency_code", "currencyCode", "currency"), ""),
        "balance": float(_api_get_value(row, ("currency_amount", "amount", "balance"), 0) or 0),
        "created_at": _api_get_date(row, ("date_time", "add_time", "update_time", "created_at", "createdAt")),
    }
    query = """
    MERGE INTO accounts AS tgt
    USING (
        SELECT
            %(id)s AS id,
            %(name)s AS name,
            %(account_type)s AS account_type,
            %(currency)s AS currency,
            %(balance)s::FLOAT AS balance,
            %(created_at)s::TIMESTAMP_LTZ AS created_at
    ) AS src
    ON tgt.id = src.id
    WHEN MATCHED THEN UPDATE SET
        name = src.name,
        account_type = src.account_type,
        currency = src.currency,
        balance = src.balance
    WHEN NOT MATCHED THEN INSERT (id, name, account_type, currency, balance, created_at)
    VALUES (src.id, src.name, src.account_type, src.currency, src.balance, COALESCE(src.created_at, CURRENT_TIMESTAMP()))
    """
    with get_connection().cursor() as cur:
        execute_query(cur, query, params)


def sync_transaction_row(row: Dict[str, Any]) -> int:
    transaction_id = _api_get_value(row, ("id", "transaction_id", "transactionId"))
    if transaction_id is None:
        return 0

    # transaction type: 1=income, 2=expense, 3=transfer
    ttype_raw = _api_get_value(row, ("type",), None)
    try:
        transaction_type = int(ttype_raw) if ttype_raw is not None and str(ttype_raw).isdigit() else None
    except Exception:
        transaction_type = None

    from_acc = _api_get_value(row, ("from_account_id", "fromAccountId"), None)
    to_acc = _api_get_value(row, ("to_account_id", "toAccountId"), None)
    if from_acc == "":
        from_acc = None
    if to_acc == "":
        to_acc = None

    # prefer account_id for simple mapping (non-transfer)
    account_id = None
    if transaction_type == TRANSACTION_TYPE_TRANSFER:
        account_id = None
    else:
        account_id = from_acc or to_acc or _api_get_value(row, ("account_id", "accountId"), None)

    params = {
        "id": transaction_id,
        "posted_at": _api_get_date(row, ("date_time", "date", "posted_at", "postedAt", "add_time")),
        "description": _api_get_value(row, ("description", "remark", "note"), ""),
        "category": _api_get_value(row, ("category", "category_name", "categoryName", "income_expenditure_category_id"), ""),
        "amount": float(_api_get_value(row, ("amount", "foreign_currency_amount", "account_currency_amount"), 0) or 0),
        "currency": _api_get_value(row, ("currency_code", "currencyCode", "currency", "foreign_currency_id", "account_currency_id"), ""),
        "account_id": account_id,
        "from_account_id": from_acc,
        "to_account_id": to_acc,
        "transaction_type": transaction_type,
        "created_at": _api_get_date(row, ("date_time", "add_time", "update_time", "created_at", "createdAt")),
    }

    with get_connection().cursor() as cur:
        execute_query(
            cur,
            "SELECT 1 FROM transactions WHERE id = %(id)s",
            {"id": params["id"]},
        )
        if cur.fetch_pandas_all().empty:
            insert_query = """
            INSERT INTO transactions (id, posted_at, description, category, amount, currency, account_id, from_account_id, to_account_id, transaction_type, created_at)
            SELECT
                %(id)s,
                %(posted_at)s::DATE,
                %(description)s,
                %(category)s,
                %(amount)s::FLOAT,
                %(currency)s,
                %(account_id)s::STRING,
                %(from_account_id)s::STRING,
                %(to_account_id)s::STRING,
                %(transaction_type)s::INTEGER,
                COALESCE(%(created_at)s::TIMESTAMP_LTZ, CURRENT_TIMESTAMP())
            """
            execute_query(cur, insert_query, params)
            return 1
    return 0


def sync_money_api_data() -> Dict[str, int]:
    accounts = fetch_api_accounts()
    transaction_rows = fetch_api_transactions()

    synced_accounts = 0
    for row in accounts:
        sync_account_row(row)
        synced_accounts += 1

    synced_transactions = 0
    for row in transaction_rows:
        synced_transactions += sync_transaction_row(row)

    with get_connection().cursor() as cur:
        execute_query(
            cur,
            "MERGE INTO sync_state AS tgt USING (SELECT 'money_api' AS dataset) AS src ON tgt.dataset = src.dataset "
            "WHEN MATCHED THEN UPDATE SET last_synced_at = CURRENT_TIMESTAMP(), row_count = %(row_count)s, updated_at = CURRENT_TIMESTAMP() "
            "WHEN NOT MATCHED THEN INSERT (dataset, last_synced_at, row_count) VALUES ('money_api', CURRENT_TIMESTAMP(), %(row_count)s)",
            {"row_count": synced_accounts + synced_transactions},
        )

    return {"accounts_synced": synced_accounts, "transactions_inserted": synced_transactions}


@st.cache_data(ttl=300)
def load_accounts() -> pd.DataFrame:
    query = """
    SELECT id, name, account_type, currency, balance, created_at
    FROM accounts
    ORDER BY name
    """
    with get_connection().cursor() as cur:
        cur.execute(query)
        df = cur.fetch_pandas_all()
        return _normalize_df_columns(df)


@st.cache_data(ttl=300)
def load_transactions(account_id: Optional[str] = None, unassigned: bool = False) -> pd.DataFrame:
    conditions = []
    params = {}
    if unassigned:
        conditions.append("t.account_id IS NULL")
    elif account_id is not None:
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
        t.from_account_id,
        t.to_account_id,
        t.transaction_type,
        t.created_at
    FROM transactions t
    LEFT JOIN accounts a ON t.account_id = a.id
    {where_clause}
    ORDER BY t.posted_at DESC
    """
    with get_connection().cursor() as cur:
        if params:
            execute_query(cur, query, params)
        else:
            cur.execute(query)
        df = cur.fetch_pandas_all()
        return _normalize_df_columns(df)


@st.cache_data(ttl=300)
def load_budgets() -> pd.DataFrame:
    query = """
    SELECT id, category, amount, period, created_at
    FROM budgets
    ORDER BY category
    """
    with get_connection().cursor() as cur:
        cur.execute(query)
        df = cur.fetch_pandas_all()
        return _normalize_df_columns(df)


def add_account(name: str, account_type: str, currency: str, balance: float):
    query = """
    INSERT INTO accounts (name, account_type, currency, balance)
    VALUES (%(name)s, %(account_type)s, %(currency)s, %(balance)s)
    """
    with get_connection().cursor() as cur:
        execute_query(
            cur,
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
    account_id: Optional[str],
):
    query = """
    INSERT INTO transactions (posted_at, description, category, amount, currency, account_id)
    VALUES (%(posted_at)s, %(description)s, %(category)s, %(amount)s, %(currency)s, %(account_id)s)
    """
    with get_connection().cursor() as cur:
        execute_query(
            cur,
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
        execute_query(
            cur,
            query,
            {"category": category, "amount": amount, "period": period},
        )
    st.cache_data.clear()
    st.success("Budget saved.")


def format_money(value: float, currency: str = "VND") -> str:
    return f"{currency} {value:,.2f}"


def _normalize_df_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.copy()
    df.columns = [str(c).lower() for c in df.columns]
    return df


def show_dashboard():
    st.header("Family Finance Dashboard")
    transactions = load_transactions()
    accounts = load_accounts()

    total_balance = accounts["balance"].dropna().sum() if not accounts.empty else 0.0
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
            .groupby(pd.Grouper(key="posted_at", freq="ME"))["amount"]
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
    account_options = {row["name"]: row["ID"] for _, row in accounts.iterrows()} if not accounts.empty else {}

    with st.expander("Add new transaction"):
        posted_at = st.date_input("Date", datetime.date.today())
        description = st.text_input("Description")
        category = st.text_input("Category", value="Groceries")
        amount = st.number_input("Amount", value=0.0, format="%f")
        currency_options = sorted(accounts["currency"].dropna().unique()) if not accounts.empty else ["VND"]
        currency = st.selectbox("Currency", options=currency_options)
        account_choices = ["Unassigned"] + list(account_options.keys())
        account_name = st.selectbox("Account", options=account_choices)
        if st.button("Save transaction"):
            add_transaction(
                posted_at,
                description,
                category,
                amount,
                currency,
                None if account_name == "Unassigned" else account_options[account_name],
            )

    filter_options = ["All", "Unassigned"] + list(account_options.keys())
    selected_account = st.selectbox("Filter by account", options=filter_options)
    if selected_account == "All":
        transactions = load_transactions()
    elif selected_account == "Unassigned":
        transactions = load_transactions(unassigned=True)
    else:
        transactions = load_transactions(account_options[selected_account])

    st.dataframe(transactions)


def show_accounts_page():
    st.header("Accounts")
    accounts = load_accounts()

    if accounts.empty:
        st.info("No accounts found. Add a new account below to start tracking balances.")
    else:
        account_view = accounts
        if "NAME" in accounts.columns and "BALANCE" in accounts.columns:
            account_view = accounts[["NAME", "BALANCE"]].rename(
                columns={"NAME": "Name", "BALANCE": "Amount"}
            )
        elif "name" in accounts.columns and "balance" in accounts.columns:
            account_view = accounts[["name", "balance"]].rename(
                columns={"name": "Name", "balance": "Amount"}
            )
        else:
            account_view = accounts

        st.subheader("Account view")
        st.dataframe(account_view)

        total_balance_by_currency = (
            accounts.dropna(subset=["BALANCE"]).groupby("CURRENCY")["BALANCE"].sum().reset_index()
        )
        overall_balance = accounts["BALANCE"].dropna().sum()

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
        currency = st.text_input("Currency", value="VND")
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


def show_data_sync_page():
    st.header("Money API Sync")
    st.write(
        "Fetch the latest Money API data and sync it into the Snowflake tables we created: "
        "`accounts`, `transactions`, and `sync_state`."
    )

    if st.button("Fetch Money API data"):
        try:
            initialize_schema()
            stats = sync_money_api_data()
            st.success(
                f"Fetch complete: {stats['accounts_synced']} account rows processed, "
                f"{stats['transactions_inserted']} new transactions inserted."
            )
            st.cache_data.clear()
        except Exception as exc:
            st.error("Money API sync failed.")
            st.error(str(exc))


def main():
    st.set_page_config(page_title="Family Finance", page_icon="💰", layout="wide")
    st.title("Family Finance Manager")

    initialize_schema()

    page = st.sidebar.selectbox(
        "Choose page",
        ["Dashboard", "Transactions", "Accounts", "Budgets", "Money API Sync"],
    )
    if page == "Dashboard":
        show_dashboard()
    elif page == "Transactions":
        show_transactions_page()
    elif page == "Accounts":
        show_accounts_page()
    elif page == "Budgets":
        show_budgets_page()
    elif page == "Money API Sync":
        show_data_sync_page()


if __name__ == "__main__":
    main()
