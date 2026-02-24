import csv
from pathlib import Path

import pandas as pd
from pandas.errors import EmptyDataError


DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)


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
]
WORKER_QUOTES_COLUMNS = [
    "QUOTE_ID",
    "PART NO",
    "SUPPLIER",
    "PRICE",
    "COND_AVAILABLE",
    "QTY_AVAILABLE",
    "LT",
    "CERTIFICATE",
    "REMARKS",
    "WORKER_ID",
    "SUBMITTED_DATE",
]
FINAL_QUOTES_COLUMNS = [
    "QUOTE_ID",
    "PART NO",
    "PRICE",
    "MARGIN_PERCENT",
    "FINAL_UNIT_PRICE",
    "FINAL_TOTAL",
    "GENERATED_DATE",
]


# -------------------
# INITIALIZE FILES
# -------------------
def initialize_file(path: Path, columns: list[str]):
    if not path.exists():
        pd.DataFrame(columns=columns).to_csv(path, index=False)


def migrate_quotes_file_and_schema():
    if not QUOTES_PATH.exists():
        pd.DataFrame(columns=QUOTES_COLUMNS).to_csv(QUOTES_PATH, index=False, quoting=csv.QUOTE_ALL)
        return

    try:
        df = pd.read_csv(QUOTES_PATH)
    except EmptyDataError:
        pd.DataFrame(columns=QUOTES_COLUMNS).to_csv(QUOTES_PATH, index=False, quoting=csv.QUOTE_ALL)
        return

    for col in QUOTES_COLUMNS:
        if col not in df.columns:
            df[col] = None

    df = df[QUOTES_COLUMNS]
    df.to_csv(QUOTES_PATH, index=False, quoting=csv.QUOTE_ALL)


def migrate_assignments_schema():
    if not ASSIGNMENTS_PATH.exists():
        return

    try:
        df = pd.read_csv(ASSIGNMENTS_PATH)
    except EmptyDataError:
        pd.DataFrame(columns=ASSIGNMENTS_COLUMNS).to_csv(ASSIGNMENTS_PATH, index=False, quoting=csv.QUOTE_ALL)
        return

    for col in ASSIGNMENTS_COLUMNS:
        if col not in df.columns:
            df[col] = None

    df = df[ASSIGNMENTS_COLUMNS]
    df.to_csv(ASSIGNMENTS_PATH, index=False, quoting=csv.QUOTE_ALL)


def migrate_worker_quotes_schema():
    if not WORKER_QUOTES_PATH.exists():
        return

    try:
        df = pd.read_csv(WORKER_QUOTES_PATH)
    except EmptyDataError:
        pd.DataFrame(columns=WORKER_QUOTES_COLUMNS).to_csv(
            WORKER_QUOTES_PATH,
            index=False,
            quoting=csv.QUOTE_ALL,
        )
        return

    rename_map = {
        "COST_PRICE_EA": "PRICE",
        "COST": "COST_PRICE_EA",
        "COND": "COND_AVAILABLE",
        "LEAD_TIME": "LT",
        "REMARK": "REMARKS",
    }
    df = df.rename(columns=rename_map)
    if "COST_PRICE_EA" in df.columns and "PRICE" not in df.columns:
        df["PRICE"] = df["COST_PRICE_EA"]

    for col in WORKER_QUOTES_COLUMNS:
        if col not in df.columns:
            df[col] = None

    df = df[WORKER_QUOTES_COLUMNS]
    df.to_csv(WORKER_QUOTES_PATH, index=False, quoting=csv.QUOTE_ALL)


def migrate_final_quotes_schema():
    if not FINAL_QUOTES_PATH.exists():
        return

    try:
        df = pd.read_csv(FINAL_QUOTES_PATH)
    except EmptyDataError:
        pd.DataFrame(columns=FINAL_QUOTES_COLUMNS).to_csv(
            FINAL_QUOTES_PATH,
            index=False,
            quoting=csv.QUOTE_ALL,
        )
        return

    if "WORKER_COST" in df.columns and "PRICE" not in df.columns:
        df["PRICE"] = df["WORKER_COST"]

    for col in FINAL_QUOTES_COLUMNS:
        if col not in df.columns:
            df[col] = None

    df = df[FINAL_QUOTES_COLUMNS]
    df.to_csv(FINAL_QUOTES_PATH, index=False, quoting=csv.QUOTE_ALL)


def get_next_quote_id() -> int:
    df = read_csv(QUOTES_PATH)
    if df.empty:
        return 1

    df["QUOTE_ID"] = pd.to_numeric(df["QUOTE_ID"], errors="coerce")
    return int(df["QUOTE_ID"].max()) + 1


initialize_file(ASSIGNMENTS_PATH, ASSIGNMENTS_COLUMNS)
initialize_file(WORKER_QUOTES_PATH, WORKER_QUOTES_COLUMNS)
initialize_file(FINAL_QUOTES_PATH, FINAL_QUOTES_COLUMNS)
initialize_file(USERS_PATH, USERS_COLUMNS)
migrate_quotes_file_and_schema()
initialize_file(QUOTES_PATH, QUOTES_COLUMNS)
migrate_assignments_schema()
migrate_worker_quotes_schema()
migrate_final_quotes_schema()


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
        df = pd.read_csv(path)
        for col in df.columns:
            if "DATE" in col.upper():
                parsed = pd.to_datetime(df[col], errors="coerce")
                if parsed.notna().any():
                    df[col] = parsed.dt.strftime("%Y-%m-%d")
                    df[col] = df[col].where(parsed.notna(), "")
        return df
    except EmptyDataError:
        return pd.DataFrame()


def enforce_schema(df: pd.DataFrame, expected_columns: list[str]) -> pd.DataFrame:
    for col in expected_columns:
        if col not in df.columns:
            df[col] = None
    return df[expected_columns]
