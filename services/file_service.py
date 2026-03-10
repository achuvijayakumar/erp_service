import csv
import os
from pathlib import Path

import pandas as pd
from pandas.errors import EmptyDataError, ParserError


BASE_DIR = Path(__file__).resolve().parent.parent
erp_data_dir = os.getenv("ERP_DATA_DIR")
if erp_data_dir:
    configured_data_dir = Path(erp_data_dir).expanduser()
    DATA_DIR = configured_data_dir if configured_data_dir.is_absolute() else (BASE_DIR / configured_data_dir).resolve()
else:
    DATA_DIR = (BASE_DIR / "data").resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)

CERTIFICATE_DIR = DATA_DIR / "certificates"
CERTIFICATE_DIR.mkdir(parents=True, exist_ok=True)

USERS_PATH = DATA_DIR / "users.csv"
QUOTES_PATH = DATA_DIR / "quotes.csv"
ASSIGNMENTS_PATH = DATA_DIR / "assignments.csv"
WORKER_QUOTES_PATH = DATA_DIR / "worker_submissions.csv"
FINAL_QUOTES_PATH = DATA_DIR / "final_quotes.csv"


# -------------------
# SCHEMA DEFINITIONS
# -------------------
USERS_COLUMNS = [
    "USER_ID",
    "USERNAME",
    "ROLE",
    "PASSWORD",
]
QUOTES_COLUMNS = [
    "QUOTE_ID",
    "SL NO",
    "DATE",
    "CUSTOMER NAME",
    "CUSTOMER ID",
    "Customer ref NO",
    "PART NO",
    "DESCRIPTION",
    "COND",
    "QTY",
    "DUE DATE",
    "STATUS",
    "CREATED_DATE",
]
ASSIGNMENTS_COLUMNS = [
    "QUOTE_ID",
    "PART NO",
    "ASSIGNED_TO",
    "ASSIGNED_DATE",
    "WORKER_DUE_DATE",
]
WORKER_QUOTES_COLUMNS = [
    "SUBMISSION_ID",
    "QUOTE_ID",
    "PART NO",
    "SUPPLIER",
    "SUPPLIER_COUNTRY",
    "SUPPLIER_SOURCE",
    "PRICE",
    "COND_AVAILABLE",
    "QTY_AVAILABLE",
    "LT",
    "CERTIFICATE_AVAILABLE",
    "CERTIFICATE_FILE",
    "CERTIFICATE_TYPE",
    "REMARKS",
    "WORKER_ID",
    "SUBMITTED_DATE",
    "EDIT_REQUIRED",
    "NO_QUOTE",
    "NO_QUOTE_REMARK",
]
FINAL_QUOTES_COLUMNS = [
    "QUOTE_ID",
    "PART NO",
    "SUPPLIER",
    "PRICE",
    "MARGIN_PERCENT",
    "FINAL_UNIT_PRICE",
    "FINAL_TOTAL",
    "SELECTED_SUBMISSION_ID",
    "GENERATED_DATE",
]


# -------------------
# INITIALIZE FILES
# -------------------
def safe_read_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except (EmptyDataError, ParserError):
        # Fall back to Python engine and skip malformed rows (e.g. broken quotes).
        return pd.read_csv(path, engine="python", on_bad_lines="skip")


def ensure_users_file():
    if not USERS_PATH.exists():
        pd.DataFrame(columns=USERS_COLUMNS).to_csv(
            USERS_PATH,
            index=False,
            quoting=csv.QUOTE_ALL,
        )


def ensure_quotes_file():
    if not QUOTES_PATH.exists():
        pd.DataFrame(columns=QUOTES_COLUMNS).to_csv(
            QUOTES_PATH,
            index=False,
            quoting=csv.QUOTE_ALL,
        )


def ensure_assignments_file():
    if not ASSIGNMENTS_PATH.exists():
        pd.DataFrame(columns=ASSIGNMENTS_COLUMNS).to_csv(
            ASSIGNMENTS_PATH,
            index=False,
            quoting=csv.QUOTE_ALL,
        )


def ensure_worker_submissions_file():
    if not WORKER_QUOTES_PATH.exists():
        pd.DataFrame(columns=WORKER_QUOTES_COLUMNS).to_csv(
            WORKER_QUOTES_PATH,
            index=False,
            quoting=csv.QUOTE_ALL,
        )


def ensure_final_quotes_file():
    if not FINAL_QUOTES_PATH.exists():
        pd.DataFrame(columns=FINAL_QUOTES_COLUMNS).to_csv(
            FINAL_QUOTES_PATH,
            index=False,
            quoting=csv.QUOTE_ALL,
        )


def get_next_quote_id() -> int:
    df = read_csv(QUOTES_PATH)
    if df.empty:
        return 1

    df["QUOTE_ID"] = pd.to_numeric(df["QUOTE_ID"], errors="coerce")
    return int(df["QUOTE_ID"].max()) + 1


ensure_users_file()
ensure_quotes_file()
ensure_assignments_file()
ensure_worker_submissions_file()
ensure_final_quotes_file()


# -------------------
# PERSISTENCE
# -------------------
def append_to_csv(path: Path, row: dict, columns: list[str]):
    df = pd.DataFrame([row])
    for col in columns:
        if col not in df.columns:
            df[col] = None
    df = df[columns]
    df.to_csv(path, mode="a", header=False, index=False, quoting=csv.QUOTE_ALL)


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()

    try:
        df = safe_read_csv(path)
        for col in df.columns:
            if "DATE" in col.upper():
                parsed = pd.to_datetime(df[col], errors="coerce")
                if parsed.notna().any():
                    df[col] = parsed.dt.strftime("%Y-%m-%d")
                    df[col] = df[col].where(parsed.notna(), "")
        return df
    except (EmptyDataError, ParserError):
        return pd.DataFrame()


def enforce_schema(df: pd.DataFrame, expected_columns: list[str]) -> pd.DataFrame:
    for col in expected_columns:
        if col not in df.columns:
            df[col] = None
    return df[expected_columns]
