import streamlit as st
import json
import uuid
import pandas as pd
import csv
import re
import io
import zipfile
from io import BytesIO
from datetime import datetime
from pathlib import Path

from services.file_service import (
    append_to_csv,
    read_csv,

    USERS_PATH,
    QUOTES_PATH,
    ASSIGNMENTS_PATH,
    WORKER_QUOTES_PATH,
    FINAL_QUOTES_PATH,
    CERTIFICATE_DIR,

    QUOTES_COLUMNS,
    ASSIGNMENTS_COLUMNS,
    WORKER_QUOTES_COLUMNS,
    FINAL_QUOTES_COLUMNS,
    enforce_schema
)

st.set_page_config(layout="wide")


def to_excel_bytes(df: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()


def render_export_button(
    label: str,
    df: pd.DataFrame,
    filename: str,
    mime: str = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
):
    if df is None or df.empty:
        st.info(f"No data available for {label.lower()}.")
        return
    st.download_button(label, to_excel_bytes(df), filename, mime)


def render_import_replace_csv(label: str, expected_columns: list[str], target_path: Path, success_msg: str, key: str):
    uploaded_csv = st.file_uploader(label, type=["xlsx", "xls", "csv"], key=key)
    if uploaded_csv is None:
        return

    try:
        filename = str(uploaded_csv.name).lower()
        if filename.endswith(".csv"):
            df = pd.read_csv(uploaded_csv)
        else:
            df = pd.read_excel(uploaded_csv)
    except Exception as e:
        st.error(f"Invalid file: {e}")
        return

    for col in expected_columns:
        if col not in df.columns:
            df[col] = None
    df = df[expected_columns]

    df.to_csv(target_path, index=False, quoting=csv.QUOTE_ALL)
    st.success(success_msg)
    st.rerun()


def apply_common_filters(df: pd.DataFrame, key_prefix: str) -> pd.DataFrame:
    df = df.copy()
    df["STATUS"] = df["STATUS"].astype(str).str.strip().str.upper()

    with st.expander("Filters", expanded=False):

        col1, col2, col3 = st.columns(3)

        with col1:
            quote_filter = st.text_input(
                "Filter by QUOTE_ID",
                key=f"{key_prefix}_quote_filter"
            )

        with col2:
            ref_filter = st.text_input(
                "Filter by Customer Ref",
                key=f"{key_prefix}_ref_filter"
            )

        with col3:
            status_options = sorted(
                df["STATUS"].unique().tolist()
            )
            status_filter = st.multiselect(
                "Filter by Status",
                status_options,
                key=f"{key_prefix}_status_filter"
            )

    df["QUOTE_ID"] = df["QUOTE_ID"].astype(str)
    df["Customer ref NO"] = df["Customer ref NO"].astype(str)

    if quote_filter:
        df = df[df["QUOTE_ID"].str.contains(quote_filter, case=False, na=False)]

    if ref_filter:
        df = df[df["Customer ref NO"].str.contains(ref_filter, case=False, na=False)]

    if status_filter:
        df = df[df["STATUS"].isin(status_filter)]

    return df


def apply_part_details_filters(df: pd.DataFrame, key_prefix: str) -> pd.DataFrame:
    df = df.copy()

    with st.expander("Filters", expanded=False):
        col1, col2, col3 = st.columns(3)

        with col1:
            quote_filter = st.text_input(
                "Filter by QUOTE_ID",
                key=f"{key_prefix}_quote"
            )

        with col2:
            ref_filter = st.text_input(
                "Filter by Customer Ref",
                key=f"{key_prefix}_ref"
            )

        with col3:
            part_filter = st.text_input(
                "Filter by PART NO",
                key=f"{key_prefix}_part"
            )

        col4, col5 = st.columns(2)
        with col4:
            worker_options = sorted(
                df["WORKER_NAME"].dropna().astype(str).unique().tolist()
            ) if "WORKER_NAME" in df.columns else []
            worker_filter = st.multiselect(
                "Filter by Worker",
                worker_options,
                key=f"{key_prefix}_worker"
            )

        with col5:
            supplier_filter = st.text_input(
                "Filter by Supplier",
                key=f"{key_prefix}_supplier"
            )

    if quote_filter:
        df = df[df["QUOTE_ID"].astype(str).str.contains(quote_filter, case=False, na=False)]

    if ref_filter and "Customer ref NO" in df.columns:
        df = df[df["Customer ref NO"].astype(str).str.contains(ref_filter, case=False, na=False)]

    if part_filter:
        df = df[df["PART NO"].astype(str).str.contains(part_filter, case=False, na=False)]

    if worker_filter and "WORKER_NAME" in df.columns:
        df = df[df["WORKER_NAME"].astype(str).isin(worker_filter)]

    if supplier_filter and "SUPPLIER" in df.columns:
        df = df[df["SUPPLIER"].astype(str).str.contains(supplier_filter, case=False, na=False)]

    return df


def apply_assigned_parts_filters(df: pd.DataFrame, key_prefix: str) -> pd.DataFrame:
    df = df.copy()

    with st.expander("Filters", expanded=False):
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            quote_filter = st.text_input("Filter by QUOTE_ID", key=f"{key_prefix}_quote")
        with col2:
            ref_filter = st.text_input("Filter by Customer Ref", key=f"{key_prefix}_ref")
        with col3:
            part_filter = st.text_input("Filter by PART NO", key=f"{key_prefix}_part")
        with col4:
            due_filter = st.text_input("Filter by Worker Due Date", key=f"{key_prefix}_due")

    if quote_filter:
        df = df[df["QUOTE_ID"].astype(str).str.contains(quote_filter, case=False, na=False)]
    if ref_filter:
        df = df[df["Customer ref NO"].astype(str).str.contains(ref_filter, case=False, na=False)]
    if part_filter:
        df = df[df["PART NO"].astype(str).str.contains(part_filter, case=False, na=False)]
    if due_filter and "WORKER_DUE_DATE" in df.columns:
        df = df[df["WORKER_DUE_DATE"].astype(str).str.contains(due_filter, case=False, na=False)]

    return df


def apply_my_submissions_filters(df: pd.DataFrame, key_prefix: str) -> pd.DataFrame:
    df = df.copy()

    with st.expander("Filters", expanded=False):
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            ref_filter = st.text_input("Filter by Customer Ref", key=f"{key_prefix}_ref")
        with col2:
            part_filter = st.text_input("Filter by PART NO", key=f"{key_prefix}_part")
        with col3:
            condition_options = sorted(df["CONDITION"].dropna().astype(str).unique().tolist()) if "CONDITION" in df.columns else []
            condition_filter = st.multiselect("Filter by Condition", condition_options, key=f"{key_prefix}_condition")
        with col4:
            submitted_date_filter = st.text_input("Filter by Submitted Date", key=f"{key_prefix}_submitted_date")

    if ref_filter:
        df = df[df["REF NO"].astype(str).str.contains(ref_filter, case=False, na=False)]
    if part_filter:
        df = df[df["PART NO"].astype(str).str.contains(part_filter, case=False, na=False)]
    if condition_filter and "CONDITION" in df.columns:
        df = df[df["CONDITION"].astype(str).isin(condition_filter)]
    if submitted_date_filter and "SUBMITTED DATE" in df.columns:
        df = df[df["SUBMITTED DATE"].astype(str).str.contains(submitted_date_filter, case=False, na=False)]

    return df


def build_quote_id(customer_name, customer_ref) -> str:
    name = "" if pd.isna(customer_name) else str(customer_name).strip()
    ref = "" if pd.isna(customer_ref) else str(customer_ref).strip()
    name = re.sub(r"\s+", " ", name) if name else "UNKNOWN"
    ref = re.sub(r"\s+", " ", ref) if ref else "NOREF"
    return f"{name}_{ref}"


def safe_filename_part(value: str) -> str:
    raw = "" if pd.isna(value) else str(value).strip()
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", raw)
    safe = safe.strip("._")
    return safe or "NA"


def clean_text(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def make_unique_label(base_label: str, disambiguation_values: list[str], existing_labels: set[str]) -> str:
    base_label = clean_text(base_label) or "N/A"
    if base_label not in existing_labels:
        return base_label

    for value in disambiguation_values:
        suffix = clean_text(value)
        if not suffix:
            continue
        candidate = f"{base_label} | {suffix}"
        if candidate not in existing_labels:
            return candidate

    suffix_index = 2
    while True:
        candidate = f"{base_label} (#{suffix_index})"
        if candidate not in existing_labels:
            return candidate
        suffix_index += 1


def update_quote_status_if_fully_submitted(quote_id: str):
    quotes_df = read_csv(QUOTES_PATH)
    worker_df = read_csv(WORKER_QUOTES_PATH)

    if quotes_df.empty:
        return False

    # Normalize keys
    quotes_df["QUOTE_ID"] = quotes_df["QUOTE_ID"].astype(str).str.strip()
    quotes_df["PART NO"] = quotes_df["PART NO"].astype(str).str.strip()

    if worker_df.empty:
        submitted_parts = 0
    else:
        worker_df["QUOTE_ID"] = worker_df["QUOTE_ID"].astype(str).str.strip()
        worker_df["PART NO"] = worker_df["PART NO"].astype(str).str.strip()
        submitted_parts = worker_df[
            worker_df["QUOTE_ID"] == str(quote_id).strip()
        ]["PART NO"].nunique()

    quote_rows = quotes_df[quotes_df["QUOTE_ID"] == str(quote_id).strip()]
    total_parts = quote_rows["PART NO"].nunique()

    if quote_rows.empty:
        return False

    current_statuses = quote_rows["STATUS"].astype(str).str.strip().str.upper().unique().tolist()
    if any(s in {"DELETED", "COMPLETED"} for s in current_statuses):
        return False

    # Only move forward if ALL parts submitted
    if total_parts > 0 and submitted_parts >= total_parts:
        quotes_df.loc[
            quotes_df["QUOTE_ID"] == str(quote_id).strip(),
            "STATUS"
        ] = "SUBMITTED"

        quotes_df.to_csv(QUOTES_PATH, index=False, quoting=csv.QUOTE_ALL)
        return True

    return False

if "is_authenticated" not in st.session_state:
    st.session_state.is_authenticated = False
    st.session_state.user_id = None
    st.session_state.username = None
    st.session_state.role = None

users_df = read_csv(USERS_PATH)

if not st.session_state.is_authenticated:
    st.title("ERP Login")

    usernames = users_df["USERNAME"].tolist()

    selected_username = st.selectbox(
        "Select User",
        usernames
    )

    password = st.text_input(
        "Password",
        type="password"
    )

    if st.button("Login"):

        user_row = users_df[
            users_df["USERNAME"] == selected_username
        ].iloc[0]

        stored_password = str(user_row["PASSWORD"])

        if password != stored_password:
            st.error("Invalid password")
            st.stop()

        st.session_state.is_authenticated = True
        st.session_state.user_id = user_row["USER_ID"]
        st.session_state.username = user_row["USERNAME"]
        st.session_state.role = user_row["ROLE"]

        st.rerun()

    st.stop()

role = st.session_state.role
user_id = st.session_state.user_id
username = st.session_state.username

col1, col2 = st.columns([6, 1])
with col1:
    st.title("Admin View" if role == "admin" else "Worker View")
with col2:
    if st.button("Logout"):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()

if role == "admin":
    tabs = st.tabs([
        "Upload Quotes",
        "Master Quotes",
        "Assign Quotes",
        "Part Details",
        "Margin & Internal Quote",
        "Export Client Quote"
    ])
else:
    tabs = st.tabs([
         "Assigned (Pending)",
        "Submit Supplier Info",
        "My Submitted Quotes"
    ])
# -----------------------------------------------
def parse_excel(file) -> pd.DataFrame:
    df = pd.read_excel(file)

    df.columns = [col.strip() for col in df.columns]

    required_columns = [
        "SL NO",
        "DATE",
        "CUSTOMER NAME",
        "CUSTOMER ID",
        "Customer ref NO",
        "PART NO",
        "DESCRIPTION",
        "QTY",
        "COND",
        "DUE DATE"
    ]

    missing = [c for c in required_columns if c not in df.columns]

    if missing:
        raise ValueError(f"Missing columns: {missing}")

    return df[required_columns]
# -----------------------------------------------
if role == "admin":

    # -------------------------
    # TAB 1 - Upload Quotes
    # -------------------------
    with tabs[0]:
        #st.header("Upload Quotes")

        upload_template_df = pd.DataFrame(columns=[
            "SL NO",
            "DATE",
            "CUSTOMER NAME",
            "CUSTOMER ID",
            "Customer ref NO",
            "PART NO",
            "DESCRIPTION",
            "QTY",
            "COND",
            "DUE DATE"
        ])
        st.download_button(
            "Download Template",
            to_excel_bytes(upload_template_df),
            "quotes_upload_template.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        uploaded_file = st.file_uploader(
            "Upload Excel File",
            type=["xlsx", "xls"]
        )

        parsed_df = pd.DataFrame(columns=[
            "QUOTE_ID",
            "SL NO",
            "DATE",
            "CUSTOMER NAME",
            "CUSTOMER ID",
            "Customer ref NO",
            "PART NO",
            "DESCRIPTION",
            "QTY",
            "COND",
            "DUE DATE"
        ])

        if uploaded_file:
            try:
                parsed_df = parse_excel(uploaded_file)
            except ValueError as e:
                st.error(str(e))
                parsed_df = pd.DataFrame(columns=[
                    "QUOTE_ID",
                    "SL NO",
                    "DATE",
                    "CUSTOMER NAME",
                    "CUSTOMER ID",
                    "Customer ref NO",
                    "PART NO",
                    "DESCRIPTION",
                    "QTY",
                    "COND",
                    "DUE DATE"
                ])

            # Clean QTY
            if not parsed_df.empty:
                parsed_df["QTY"] = pd.to_numeric(
                    parsed_df["QTY"],
                    errors="coerce"
                )

                parsed_df = parsed_df.dropna(
                    subset=["PART NO", "DESCRIPTION", "QTY"]
                )

                parsed_df["QUOTE_ID"] = parsed_df.apply(
                    lambda row: build_quote_id(
                        row.get("CUSTOMER NAME"),
                        row.get("Customer ref NO")
                    ),
                    axis=1
                )
                quote_cols = ["QUOTE_ID"] + [c for c in parsed_df.columns if c != "QUOTE_ID"]
                parsed_df = parsed_df[quote_cols]

            if not parsed_df.empty:
                st.success(f"{len(parsed_df)} rows parsed successfully")

        if uploaded_file and not parsed_df.empty:
            export_excel = to_excel_bytes(parsed_df)

            st.download_button(
                "Download Parsed Data",
                export_excel,
                "parsed_quotes.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

        editor_df = parsed_df if uploaded_file else pd.DataFrame(columns=[
            "QUOTE_ID",
            "SL NO",
            "DATE",
            "CUSTOMER NAME",
            "CUSTOMER ID",
            "Customer ref NO",
            "PART NO",
            "DESCRIPTION",
            "QTY",
            "COND",
            "DUE DATE"
        ])
        for text_col in ["CUSTOMER NAME", "CUSTOMER ID"]:
            if text_col in editor_df.columns:
                editor_df[text_col] = editor_df[text_col].astype("string")

        quote_table = st.data_editor(
            editor_df,
            num_rows="dynamic",
            key="quote_editor",
            column_config={
                "CUSTOMER NAME": st.column_config.TextColumn("CUSTOMER NAME"),
                "CUSTOMER ID": st.column_config.TextColumn("CUSTOMER ID")
            }
        )

        if not quote_table.empty:
            current_export_excel = to_excel_bytes(quote_table)
            st.download_button(
                "Export Current Table",
                current_export_excel,
                "current_quotes.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

        if st.button("Save Quotes"):

            if quote_table.empty:
                st.warning("No data to save")
            else:
                quote_table = quote_table.copy()

                # Validate QTY
                quote_table["QTY"] = pd.to_numeric(
                    quote_table["QTY"], errors="coerce"
                )

                quote_table["DATE"] = pd.to_datetime(
                    quote_table["DATE"],
                    errors="coerce"
                )
                quote_table["DUE DATE"] = pd.to_datetime(
                    quote_table["DUE DATE"],
                    errors="coerce"
                )
                quote_table["COND"] = quote_table["COND"].astype("string").str.upper()

                missing_date_rows = int(quote_table["DATE"].isna().sum())
                if missing_date_rows > 0:
                    st.warning(
                        f"{missing_date_rows} row(s) have missing/invalid DATE and will be skipped."
                    )

                quote_table = quote_table.dropna(
                    subset=["PART NO", "QTY", "DATE"]
                )

                if quote_table.empty:
                    st.error("Invalid data")
                else:
                    save_blocked = False
                    quote_table["CUSTOMER NAME"] = (
                        quote_table["CUSTOMER NAME"]
                        .astype("string")
                        .str.strip()
                        .fillna("")
                    )
                    quote_table = quote_table[quote_table["CUSTOMER NAME"] != ""]
                    if quote_table.empty:
                        st.error("CUSTOMER NAME is required.")
                        save_blocked = True

                    quote_table["Customer ref NO"] = (
                        quote_table["Customer ref NO"]
                        .astype("string")
                        .str.strip()
                        .fillna("")
                    )
                    quote_table["CUSTOMER ID"] = (
                        quote_table["CUSTOMER ID"]
                        .astype("string")
                        .str.strip()
                        .fillna("")
                    )
                    quote_table = quote_table[quote_table["Customer ref NO"] != ""]
                    if quote_table.empty:
                        st.error("Customer ref NO is required.")
                        save_blocked = True

                    existing = read_csv(QUOTES_PATH)

                    if not save_blocked:
                        quote_table["DATE"] = quote_table["DATE"].dt.strftime("%Y-%m-%d")
                        quote_table["DUE DATE"] = quote_table["DUE DATE"].dt.strftime("%Y-%m-%d")
                        quote_table["QUOTE_ID"] = quote_table.apply(
                            lambda row: build_quote_id(
                                row.get("CUSTOMER NAME"),
                                row.get("Customer ref NO")
                            ),
                            axis=1
                        )

                        quote_table["STATUS"] = "UPLOADED"
                        quote_table["CREATED_DATE"] = datetime.now().strftime("%Y-%m-%d")

                        # Enforce Schema & Types
                        for col in QUOTES_COLUMNS:
                            if col not in quote_table.columns:
                                quote_table[col] = None
                        
                        quote_table["QUOTE_ID"] = quote_table["QUOTE_ID"].astype(str)

                        quote_table = quote_table[QUOTES_COLUMNS]

                        # Safe Rewrite (No append mode)
                        final_df = pd.concat([existing, quote_table])

                        final_df.to_csv(
                            QUOTES_PATH,
                            index=False,
                            quoting=csv.QUOTE_ALL
                        )

                        st.success(f"Saved {len(quote_table)} quotes")
    # ------------------------
    # TAB 2 - Master Quotes
    # -------------------------
    with tabs[1]:
        #t.header("Master Quotes")

        quotes_df = read_csv(QUOTES_PATH)

        if not quotes_df.empty:
            quotes_df = quotes_df.copy()
            quotes_df["QUOTE_ID"] = quotes_df["QUOTE_ID"].astype(str).str.strip()
            quotes_df["STATUS"] = quotes_df["STATUS"].astype(str).str.strip()
            quotes_df["STATUS_NORM"] = quotes_df["STATUS"].str.upper()

            active_quotes_df = quotes_df[
                ~quotes_df["STATUS_NORM"].isin(["COMPLETED", "DELETED"])
            ].copy()
            archive_quotes_df = quotes_df[quotes_df["STATUS_NORM"] == "COMPLETED"].copy()
            deleted_quotes_df = quotes_df[quotes_df["STATUS_NORM"] == "DELETED"].copy()

            master_active_tab, master_archive_tab, master_deleted_tab = st.tabs(["Active", "Archive", "Deleted"])

            # -----------------------
            # TAB 2 / SUB-TAB 1 - Active Quotes
            # -----------------------
            with master_active_tab:
                if active_quotes_df.empty:
                    st.info("No active quotes.")
                else:
                    filtered_df = apply_common_filters(
                        active_quotes_df.drop(columns=["STATUS_NORM"], errors="ignore"),
                        "master_active"
                    )
                    display_df = filtered_df.drop(columns=["SL NO"], errors="ignore")
                    st.dataframe(display_df)
                    render_export_button(
                        "Export Active Quotes Excel",
                        active_quotes_df.drop(columns=["STATUS_NORM"], errors="ignore"),
                        "active_quotes.xlsx"
                    )

                    quote_options = sorted(active_quotes_df["QUOTE_ID"].dropna().unique().tolist())

                    st.divider()
                    st.subheader("Delete Quotes")
                    selected_quote_ids = st.multiselect(
                        "Select QUOTE_ID(s) to delete",
                        quote_options,
                        key="master_quotes_delete_multiselect"
                    )

                    if selected_quote_ids:
                        st.warning(
                            f"This will mark {len(selected_quote_ids)} quote(s) as DELETED."
                        )

                    if st.button("Delete Selected Quotes", key="delete_selected_master_quotes"):
                        if not selected_quote_ids:
                            st.warning("Select at least one QUOTE_ID to delete.")
                        else:
                            delete_ids = {str(q).strip() for q in selected_quote_ids}

                            quotes_df = read_csv(QUOTES_PATH)
                            quotes_df["QUOTE_ID"] = quotes_df["QUOTE_ID"].astype(str).str.strip()
                            quotes_df.loc[quotes_df["QUOTE_ID"].isin(delete_ids), "STATUS"] = "DELETED"
                            quotes_df.to_csv(QUOTES_PATH, index=False, quoting=csv.QUOTE_ALL)

                            st.success(f"Marked {len(delete_ids)} quote(s) as DELETED.")
                            st.rerun()

            # -----------------------
            # TAB 2 / SUB-TAB 2 - Archive Quotes
            # -----------------------
            with master_archive_tab:
                if archive_quotes_df.empty:
                    st.info("No archived quotes.")
                else:
                    filtered_archive = apply_common_filters(
                        archive_quotes_df.drop(columns=["STATUS_NORM"], errors="ignore"),
                        "master_archive"
                    )
                    display_archive_df = filtered_archive.drop(columns=["SL NO"], errors="ignore")
                    st.dataframe(display_archive_df)
                    render_export_button(
                        "Download Completed Quotes Excel",
                        archive_quotes_df.drop(columns=["STATUS_NORM"], errors="ignore"),
                        "completed_quotes.xlsx"
                    )

            # -----------------------
            # TAB 2 / SUB-TAB 3 - Deleted Quotes
            # -----------------------
            with master_deleted_tab:
                if deleted_quotes_df.empty:
                    st.info("No deleted quotes.")
                else:
                    filtered_deleted = apply_common_filters(
                        deleted_quotes_df.drop(columns=["STATUS_NORM"], errors="ignore"),
                        "master_deleted"
                    )
                    display_deleted_df = filtered_deleted.drop(columns=["SL NO"], errors="ignore")
                    st.dataframe(display_deleted_df)
                    render_export_button(
                        "Export Deleted Quotes Excel",
                        deleted_quotes_df.drop(columns=["STATUS_NORM"], errors="ignore"),
                        "deleted_quotes.xlsx"
                    )

                    st.divider()
                    st.subheader("Permanent Cleanup")
                    confirm_clear_deleted = st.checkbox(
                        "I confirm that all deleted quotes and related records should be permanently removed.",
                        key="confirm_clear_deleted_quotes",
                    )
                    if st.button("Clear All Deleted Quotes", key="clear_all_deleted_quotes_btn"):
                        if not confirm_clear_deleted:
                            st.warning("Please confirm before clearing deleted quotes.")
                        else:
                            quotes_cleanup_df = read_csv(QUOTES_PATH)
                            quotes_cleanup_df = enforce_schema(quotes_cleanup_df, QUOTES_COLUMNS)
                            quotes_cleanup_df["QUOTE_ID"] = quotes_cleanup_df["QUOTE_ID"].astype(str).str.strip()
                            quotes_cleanup_df["STATUS"] = quotes_cleanup_df["STATUS"].astype(str).str.strip().str.upper()

                            deleted_quote_ids = set(
                                quotes_cleanup_df.loc[
                                    quotes_cleanup_df["STATUS"] == "DELETED",
                                    "QUOTE_ID",
                                ].astype(str).str.strip()
                            )
                            deleted_quote_ids.discard("")

                            if not deleted_quote_ids:
                                st.info("No deleted quotes found.")
                            else:
                                quotes_cleanup_df = quotes_cleanup_df[
                                    ~quotes_cleanup_df["QUOTE_ID"].isin(deleted_quote_ids)
                                ].copy()
                                quotes_cleanup_df.to_csv(QUOTES_PATH, index=False, quoting=csv.QUOTE_ALL)

                                assignments_cleanup_df = read_csv(ASSIGNMENTS_PATH)
                                assignments_cleanup_df = enforce_schema(assignments_cleanup_df, ASSIGNMENTS_COLUMNS)
                                assignments_cleanup_df["QUOTE_ID"] = assignments_cleanup_df["QUOTE_ID"].astype(str).str.strip()
                                assignments_cleanup_df = assignments_cleanup_df[
                                    ~assignments_cleanup_df["QUOTE_ID"].isin(deleted_quote_ids)
                                ].copy()
                                assignments_cleanup_df.to_csv(ASSIGNMENTS_PATH, index=False, quoting=csv.QUOTE_ALL)

                                worker_cleanup_df = read_csv(WORKER_QUOTES_PATH)
                                worker_cleanup_df = enforce_schema(worker_cleanup_df, WORKER_QUOTES_COLUMNS)
                                worker_cleanup_df["QUOTE_ID"] = worker_cleanup_df["QUOTE_ID"].astype(str).str.strip()
                                
                                deleted_workers = worker_cleanup_df[worker_cleanup_df["QUOTE_ID"].isin(deleted_quote_ids)]
                                if "CERTIFICATE_FILE" in deleted_workers.columns:
                                    for cert in deleted_workers["CERTIFICATE_FILE"].dropna().unique():
                                        cert_str = str(cert).strip()
                                        if cert_str:
                                            cert_name = Path(cert_str).name
                                            cert_path = CERTIFICATE_DIR / cert_name
                                            if cert_path.exists():
                                                try:
                                                    cert_path.unlink()
                                                except Exception as e:
                                                    print(f"Certificate deletion failed: {cert_path} -> {e}")

                                worker_cleanup_df = worker_cleanup_df[
                                    ~worker_cleanup_df["QUOTE_ID"].isin(deleted_quote_ids)
                                ].copy()
                                worker_cleanup_df.to_csv(WORKER_QUOTES_PATH, index=False, quoting=csv.QUOTE_ALL)

                                final_cleanup_df = read_csv(FINAL_QUOTES_PATH)
                                final_cleanup_df = enforce_schema(final_cleanup_df, FINAL_QUOTES_COLUMNS)
                                final_cleanup_df["QUOTE_ID"] = final_cleanup_df["QUOTE_ID"].astype(str).str.strip()
                                final_cleanup_df = final_cleanup_df[
                                    ~final_cleanup_df["QUOTE_ID"].isin(deleted_quote_ids)
                                ].copy()
                                final_cleanup_df.to_csv(FINAL_QUOTES_PATH, index=False, quoting=csv.QUOTE_ALL)

                                st.success("Deleted quotes permanently removed")
                                st.rerun()
        else:
            st.info("No quotes uploaded yet.")

    # -----------------------
    # TAB 3 - Assign Quotes
    # -----------------------
    with tabs[2]:
        #st.header("Assign Quotes to Workers")
        #st.subheader("Data Transfer")
        render_import_replace_csv(
            "Import Assignments Excel",
            ASSIGNMENTS_COLUMNS,
            ASSIGNMENTS_PATH,
            "Assignments file replaced successfully.",
            key="import_assignments_assign_tab"
        )
        assignments_df = read_csv(ASSIGNMENTS_PATH)
        render_export_button("Export Assignments Excel", assignments_df, "assignments.xlsx")

        quotes_df = read_csv(QUOTES_PATH)
        if not quotes_df.empty:
            quotes_df["STATUS"] = quotes_df["STATUS"].astype(str).str.strip().str.upper()
            quotes_df = quotes_df[quotes_df["STATUS"] != "DELETED"]

        if quotes_df.empty:
            st.info("No quotes available.")
        else:
            assign_pending_tab, assign_assigned_tab = st.tabs(["Pending", "Assigned"])

            # -----------------------
            # TAB 3 / SUB-TAB 1 - Pending Assignments
            # -----------------------
            with assign_pending_tab:
                pending_quote_rows = quotes_df[
                    quotes_df["STATUS"].astype(str).str.upper() == "UPLOADED"
                ].copy()
                pending_quote_rows["QTY"] = pd.to_numeric(pending_quote_rows["QTY"], errors="coerce").fillna(0)
                pending_quotes = (
                    pending_quote_rows.groupby("QUOTE_ID", as_index=False)
                    .agg(
                        PART_COUNT=("PART NO", "count"),
                        TOTAL_QTY=("QTY", "sum")
                    )
                )

                if pending_quotes.empty:
                    st.info("No unassigned quotes.")
                else:
                    worker_df = users_df[users_df["ROLE"] == "worker"].copy()
                    worker_list = worker_df["USERNAME"].tolist()

                    if not worker_list:
                        st.warning("No workers available for assignment.")
                    else:
                        h1, h2, h3, h4, h5, h6, h7 = st.columns([2.0, 1, 1.1, 1.1, 1.8, 1.6, 1])
                        with h1:
                            st.markdown("**QUOTE_ID**")
                        with h2:
                            st.markdown("**PARTS**")
                        with h3:
                            st.markdown("**TOTAL QTY**")
                        with h4:
                            st.markdown("**STATUS**")
                        with h5:
                            st.markdown("**Assign To**")
                        with h6:
                            st.markdown("**Worker Due Date**")
                        with h7:
                            st.markdown("**Action**")

                        for idx, row in pending_quotes.iterrows():
                            col1, col2, col3, col4, col5, col6, col7 = st.columns([2.0, 1, 1.1, 1.1, 1.8, 1.6, 1])

                            with col1:
                                st.write(row["QUOTE_ID"])
                            with col2:
                                st.write(int(row["PART_COUNT"]))
                            with col3:
                                st.write(row["TOTAL_QTY"])
                            with col4:
                                st.write("UPLOADED")
                            with col5:
                                selected_worker = st.selectbox(
                                    "Assign To",
                                    worker_list,
                                    key=f"worker_{idx}",
                                    label_visibility="collapsed"
                                )
                            with col6:
                                worker_due_date = st.date_input(
                                    "Worker Due Date",
                                    key=f"worker_due_{idx}",
                                    label_visibility="collapsed"
                                )
                            with col7:
                                if st.button("Assign", key=f"assign_{idx}"):
                                    existing_assignments = read_csv(ASSIGNMENTS_PATH)
                                    if existing_assignments.empty:
                                        duplicate = pd.DataFrame()
                                    else:
                                        duplicate = existing_assignments[
                                            existing_assignments["QUOTE_ID"].astype(str) == str(row["QUOTE_ID"])
                                        ]

                                    if not duplicate.empty:
                                        st.warning("This QUOTE_ID is already assigned.")
                                    else:
                                        # Double check status before committing assignment
                                        curr_quotes = read_csv(QUOTES_PATH)
                                        if not curr_quotes.empty:
                                            curr_quotes["QUOTE_ID"] = curr_quotes["QUOTE_ID"].astype(str).str.strip()
                                            q_status_rows = curr_quotes[curr_quotes["QUOTE_ID"] == str(row["QUOTE_ID"]).strip()]["STATUS"].astype(str).str.strip().str.upper()
                                            if not q_status_rows.empty and q_status_rows.iloc[0] == "COMPLETED":
                                                st.error("This RFQ is already COMPLETED and cannot be reassigned.")
                                                st.stop()
                                        
                                        worker_id = worker_df[
                                            worker_df["USERNAME"] == selected_worker
                                        ]["USER_ID"].values[0]

                                        assignment_data = {
                                            "QUOTE_ID": str(row["QUOTE_ID"]),
                                            "PART NO": "ALL",
                                            "ASSIGNED_TO": str(worker_id),
                                            "ASSIGNED_DATE": datetime.now().strftime("%Y-%m-%d"),
                                            "WORKER_DUE_DATE": worker_due_date.strftime("%Y-%m-%d")
                                        }

                                        append_to_csv(
                                            ASSIGNMENTS_PATH,
                                            assignment_data,
                                            ASSIGNMENTS_COLUMNS
                                        )

                                        quotes_df.loc[
                                            quotes_df["QUOTE_ID"].astype(str) == str(row["QUOTE_ID"]),
                                            "STATUS"
                                        ] = "ASSIGNED"

                                        quotes_df.to_csv(
                                            QUOTES_PATH,
                                            index=False,
                                            quoting=csv.QUOTE_ALL
                                        )

                                        st.success("Assigned successfully")
                                        st.rerun()

            # -----------------------
            # TAB 3 / SUB-TAB 2 - Assigned Quotes
            # -----------------------
            with assign_assigned_tab:
                if assignments_df.empty:
                    st.info("No assigned rows yet.")
                else:
                    assigned_rows = quotes_df[
                        quotes_df["STATUS"].astype(str).str.upper() == "ASSIGNED"
                    ][["QUOTE_ID", "PART NO", "DESCRIPTION", "QTY", "STATUS"]].copy()

                    if assigned_rows.empty:
                        st.info("No assigned rows yet.")
                    else:
                        assignments_view = assignments_df.copy()
                        assignments_view["QUOTE_ID"] = assignments_view["QUOTE_ID"].astype(str)
                        assignments_view["ASSIGNED_TO"] = assignments_view["ASSIGNED_TO"].astype(str)
                        if "WORKER_DUE_DATE" not in assignments_view.columns:
                            assignments_view["WORKER_DUE_DATE"] = ""
                        assignments_view["WORKER_DUE_DATE"] = assignments_view["WORKER_DUE_DATE"].astype(str).str.strip()

                        users_lookup = users_df[["USER_ID", "USERNAME"]].copy()
                        users_lookup["USER_ID"] = users_lookup["USER_ID"].astype(str)

                        assigned_rows["QUOTE_ID"] = assigned_rows["QUOTE_ID"].astype(str)
                        assigned_rows["PART NO"] = assigned_rows["PART NO"].astype(str)

                        assigned_display = assigned_rows.merge(
                            assignments_view[["QUOTE_ID", "ASSIGNED_TO", "ASSIGNED_DATE", "WORKER_DUE_DATE"]].drop_duplicates(subset=["QUOTE_ID"], keep="last"),
                            on=["QUOTE_ID"],
                            how="left"
                        ).merge(
                            users_lookup,
                            left_on="ASSIGNED_TO",
                            right_on="USER_ID",
                            how="left"
                        )

                        assigned_display = assigned_display.rename(
                            columns={"USERNAME": "ASSIGNED_TO_USER"}
                        )[[
                            "QUOTE_ID",
                            "PART NO",
                            "DESCRIPTION",
                            "QTY",
                            "STATUS",
                            "ASSIGNED_TO_USER",
                            "ASSIGNED_DATE",
                            "WORKER_DUE_DATE"
                        ]]

                        st.dataframe(assigned_display, width='stretch')

    # -----------------------
    # TAB 4 - Part Details
    # -----------------------
    with tabs[3]:
        #st.header("Part Details")
        #st.subheader("Data Transfer")
        render_import_replace_csv(
            "Import Part Details Excel",
            WORKER_QUOTES_COLUMNS,
            WORKER_QUOTES_PATH,
            "Part details file replaced successfully.",
            key="import_part_details_admin"
        )

        submissions_df = read_csv(WORKER_QUOTES_PATH)
        if not submissions_df.empty:
            if "EDIT_REQUIRED" not in submissions_df.columns:
                submissions_df["EDIT_REQUIRED"] = "NO"
            if "NO_QUOTE" not in submissions_df.columns:
                submissions_df["NO_QUOTE"] = "NO"
            submissions_df["EDIT_REQUIRED"] = submissions_df["EDIT_REQUIRED"].fillna("").astype(str).str.strip().str.upper()
            submissions_df.loc[~submissions_df["EDIT_REQUIRED"].isin(["YES", "NO"]), "EDIT_REQUIRED"] = "NO"
            submissions_df["NO_QUOTE"] = submissions_df["NO_QUOTE"].fillna("").astype(str).str.strip().str.upper()
            submissions_df.loc[~submissions_df["NO_QUOTE"].isin(["YES", "NO"]), "NO_QUOTE"] = "NO"

            users_lookup = users_df[["USER_ID", "USERNAME"]].copy()
            users_lookup["USER_ID"] = users_lookup["USER_ID"].astype(str).str.strip()
            quotes_lookup = read_csv(QUOTES_PATH)
            if not quotes_lookup.empty:
                quotes_lookup["QUOTE_ID"] = quotes_lookup["QUOTE_ID"].astype(str).str.strip()
                quotes_lookup["PART NO"] = quotes_lookup["PART NO"].astype(str).str.strip()
                quotes_lookup = quotes_lookup[["QUOTE_ID", "PART NO", "Customer ref NO", "DESCRIPTION"]].drop_duplicates()

            part_details_view = submissions_df.copy()
            part_details_view["QUOTE_ID"] = part_details_view["QUOTE_ID"].astype(str).str.strip()
            part_details_view["PART NO"] = part_details_view["PART NO"].astype(str).str.strip()
            part_details_view["WORKER_ID"] = part_details_view["WORKER_ID"].astype(str).str.strip()
            if not quotes_lookup.empty:
                part_details_view = part_details_view.merge(
                    quotes_lookup,
                    on=["QUOTE_ID", "PART NO"],
                    how="left"
                )
            part_details_view = part_details_view.merge(
                users_lookup,
                left_on="WORKER_ID",
                right_on="USER_ID",
                how="left"
            )
            part_details_view = part_details_view.rename(columns={"USERNAME": "WORKER_NAME"})
            part_details_view = part_details_view.drop(columns=["USER_ID"], errors="ignore")
            part_details_view["EDIT_REQUIRED"] = part_details_view["EDIT_REQUIRED"].fillna("").astype(str).str.strip().str.upper()
            part_details_view.loc[~part_details_view["EDIT_REQUIRED"].isin(["YES", "NO"]), "EDIT_REQUIRED"] = "NO"
            part_details_view["NO_QUOTE"] = part_details_view["NO_QUOTE"].fillna("").astype(str).str.strip().str.upper()
            part_details_view.loc[~part_details_view["NO_QUOTE"].isin(["YES", "NO"]), "NO_QUOTE"] = "NO"

            part_details_view_tab, part_details_reassign_tab, part_details_no_quote_tab = st.tabs(["View", "Re-Assign", "No Quote"])

            with part_details_view_tab:
                if st.session_state.pop("part_details_changes_saved", False):
                    st.success("Changes saved")

                visible_part_details = part_details_view[
                    (part_details_view["EDIT_REQUIRED"] == "NO") &
                    (part_details_view["NO_QUOTE"] == "NO")
                ].copy()
                if visible_part_details.empty:
                    st.info("No part details ready for review.")
                else:
                    filtered_part_details = apply_part_details_filters(
                        visible_part_details,
                        "part_details_view"
                    )
                    editor_columns = [
                        "QUOTE_ID",
                        "PART NO",
                        "SUPPLIER",
                        "SUPPLIER_COUNTRY",
                        "SUPPLIER_SOURCE",
                        "PRICE",
                        "COND_AVAILABLE",
                        "QTY_AVAILABLE",
                        "LT",
                        "REMARKS",
                        "CERTIFICATE_TYPE",
                        "WORKER_NAME",
                        "SUBMITTED_DATE",
                    ]
                    for col in editor_columns:
                        if col not in filtered_part_details.columns:
                            filtered_part_details[col] = ""
                    editor_df = filtered_part_details[editor_columns].copy()
                    text_cols = [
                        "QUOTE_ID",
                        "PART NO",
                        "SUPPLIER",
                        "SUPPLIER_COUNTRY",
                        "SUPPLIER_SOURCE",
                        "COND_AVAILABLE",
                        "LT",
                        "REMARKS",
                        "CERTIFICATE_TYPE",
                        "WORKER_NAME",
                        "SUBMITTED_DATE",
                    ]
                    for col in text_cols:
                        editor_df[col] = editor_df[col].fillna("").astype(str).str.strip()
                    editor_df["PRICE"] = pd.to_numeric(editor_df["PRICE"], errors="coerce")
                    editor_df["QTY_AVAILABLE"] = pd.to_numeric(editor_df["QTY_AVAILABLE"], errors="coerce")

                    edited_part_details = st.data_editor(
                        editor_df,
                        use_container_width=True,
                        num_rows="fixed",
                        disabled=["QUOTE_ID", "PART NO", "SUPPLIER", "WORKER_NAME", "SUBMITTED_DATE"],
                        column_config={
                            "PRICE": st.column_config.NumberColumn("PRICE", min_value=0.0, step=0.01),
                            "QTY_AVAILABLE": st.column_config.NumberColumn("QTY_AVAILABLE", min_value=0, step=1),
                            "COND_AVAILABLE": st.column_config.SelectboxColumn(
                                "COND_AVAILABLE",
                                options=["NE", "NS", "OH", "SV", "AR", "FN", "MOD","RP","IN"],
                            ),
                        },
                        key="part_details_view_editor",
                    )

                    if st.button("Save Changes", key="part_details_save_changes_btn"):
                        update_df = read_csv(WORKER_QUOTES_PATH)
                        if update_df.empty:
                            st.error("No submissions found to update.")
                        else:
                            update_df = enforce_schema(update_df, WORKER_QUOTES_COLUMNS)
                            update_df["QUOTE_ID"] = update_df["QUOTE_ID"].astype(str).str.strip()
                            update_df["PART NO"] = update_df["PART NO"].astype(str).str.strip()
                            update_df["SUPPLIER"] = update_df["SUPPLIER"].astype(str).str.strip()

                            updated_keys = set()
                            for idx, row in edited_part_details.iterrows():
                                sub_id = str(filtered_part_details.loc[idx, "SUBMISSION_ID"]).strip()
                                quote_id = str(row.get("QUOTE_ID", "")).strip()
                                part_no = str(row.get("PART NO", "")).strip()
                                supplier = str(row.get("SUPPLIER", "")).strip()
                                if not sub_id:
                                    continue

                                mask = (update_df["SUBMISSION_ID"] == sub_id)
                                if not mask.any():
                                    continue

                                price_val = pd.to_numeric(pd.Series([row.get("PRICE", None)]), errors="coerce").iloc[0]
                                qty_val = pd.to_numeric(pd.Series([row.get("QTY_AVAILABLE", None)]), errors="coerce").iloc[0]
                                update_df.loc[mask, "SUPPLIER_COUNTRY"] = str(row.get("SUPPLIER_COUNTRY", "")).strip()
                                update_df.loc[mask, "PRICE"] = None if pd.isna(price_val) else float(price_val)
                                update_df.loc[mask, "COND_AVAILABLE"] = str(row.get("COND_AVAILABLE", "")).strip()
                                update_df.loc[mask, "QTY_AVAILABLE"] = None if pd.isna(qty_val) else int(qty_val)
                                update_df.loc[mask, "LT"] = str(row.get("LT", "")).strip()
                                update_df.loc[mask, "REMARKS"] = str(row.get("REMARKS", "")).strip()
                                update_df.loc[mask, "CERTIFICATE_TYPE"] = str(row.get("CERTIFICATE_TYPE", "")).strip()
                                updated_keys.add(sub_id)

                            update_df.to_csv(WORKER_QUOTES_PATH, index=False, quoting=csv.QUOTE_ALL)
                            if not updated_keys:
                                st.warning("No matching rows were updated.")
                            else:
                                st.session_state["part_details_changes_saved"] = True
                            st.rerun()

                    cert_source_df = filtered_part_details.copy()
                    if "CERTIFICATE_FILE" not in cert_source_df.columns:
                        cert_source_df["CERTIFICATE_FILE"] = None
                    cert_source_df["CERTIFICATE_FILE"] = (
                        cert_source_df["CERTIFICATE_FILE"].fillna("").astype(str).str.strip()
                    )

                    with st.expander("Certificate Downloads", expanded=False):
                        for idx, row in cert_source_df.iterrows():
                            cert_file = str(row.get("CERTIFICATE_FILE", "")).strip()
                            if not cert_file:
                                continue

                            file_path = CERTIFICATE_DIR / cert_file
                            row_key = (
                                f"cert_download_{safe_filename_part(row.get('QUOTE_ID', ''))}_"
                                f"{safe_filename_part(row.get('PART NO', ''))}_"
                                f"{safe_filename_part(row.get('SUPPLIER', ''))}_{idx}"
                            )

                            c1, c2, c3, c4 = st.columns([2.2, 2, 2, 1.2])
                            with c1:
                                st.write(str(row.get("QUOTE_ID", "")))
                            with c2:
                                st.write(str(row.get("PART NO", "")))
                            with c3:
                                st.write(str(row.get("SUPPLIER", "")))
                            with c4:
                                if file_path.exists():
                                    with open(file_path, "rb") as f:
                                        st.download_button(
                                            label="Download",
                                            data=f.read(),
                                            file_name=cert_file,
                                            key=row_key
                                        )
                                else:
                                    st.write("File missing")

                    zip_buffer = io.BytesIO()
                    excel_bytes = to_excel_bytes(filtered_part_details)
                    with zipfile.ZipFile(zip_buffer, "w") as zf:
                        zf.writestr("part_details.xlsx", excel_bytes)
                        added_files = set()
                        for _, row in cert_source_df.iterrows():
                            cert_file = str(row.get("CERTIFICATE_FILE", "")).strip()
                            if not cert_file or cert_file in added_files:
                                continue
                            file_path = CERTIFICATE_DIR / cert_file
                            if file_path.exists():
                                zf.write(file_path, arcname=f"certificates/{cert_file}")
                                added_files.add(cert_file)

                    st.download_button(
                        "Download Part Details (ZIP)",
                        zip_buffer.getvalue(),
                        file_name="part_details.zip",
                        mime="application/zip",
                        key="part_details_zip_download"
                    )

            with part_details_reassign_tab:
                reassign_candidates = part_details_view[
                    (part_details_view["EDIT_REQUIRED"] == "NO") &
                    (part_details_view["NO_QUOTE"] == "NO")
                ].copy()
                if reassign_candidates.empty:
                    st.info("No submitted rows available to re-assign.")
                else:
                    filtered_reassign = apply_part_details_filters(
                        reassign_candidates,
                        "part_details_reassign"
                    )
                    st.dataframe(
                        filtered_reassign.drop(columns=["WORKER_ID", "SUBMISSION_ID"], errors="ignore"),
                        use_container_width=True
                    )

                    key_rows = filtered_reassign[["SUBMISSION_ID", "QUOTE_ID", "PART NO", "SUPPLIER"]].copy()
                    key_rows["SUBMISSION_ID"] = key_rows["SUBMISSION_ID"].astype(str).str.strip()
                    key_rows["QUOTE_ID"] = key_rows["QUOTE_ID"].astype(str).str.strip()
                    key_rows["PART NO"] = key_rows["PART NO"].astype(str).str.strip()
                    key_rows["SUPPLIER"] = key_rows["SUPPLIER"].astype(str).str.strip()
                    key_rows = key_rows.drop_duplicates()

                    select_options = list(key_rows.itertuples(index=False, name=None))
                    selected_rows = st.multiselect(
                        "Select row(s) to send back",
                        options=select_options,
                        format_func=lambda x: f"{x[1]} | {x[2]} | {x[3]}",
                        key="part_details_send_back_select"
                    )

                    # Worker dropdown for reassignment
                    reassign_users_df = read_csv(USERS_PATH)
                    worker_users = reassign_users_df[
                        reassign_users_df["ROLE"].astype(str).str.strip().str.lower() == "worker"
                    ].copy()
                    worker_id_name_map = dict(
                        zip(
                            worker_users["USER_ID"].astype(str).str.strip(),
                            worker_users["USERNAME"].astype(str).str.strip(),
                        )
                    )
                    worker_options = [("", "<Select Worker>")] + list(worker_id_name_map.items())

                    # Determine default worker
                    default_index = 0  # Default to "<Select Worker>"
                    if selected_rows:
                        selected_sub_ids = {str(r[0]).strip() for r in selected_rows}
                        matched_worker_ids = set()
                        for _, row in filtered_reassign.iterrows():
                            if str(row.get("SUBMISSION_ID", "")).strip() in selected_sub_ids:
                                matched_worker_ids.add(str(row.get("WORKER_ID", "")).strip())
                        
                        if len(matched_worker_ids) == 1:
                            current_wid = matched_worker_ids.pop()
                            # Search in worker_options starting from index 1
                            for idx, (wid, _) in enumerate(worker_options):
                                if wid == current_wid:
                                    default_index = idx
                                    break

                    reassign_worker = st.selectbox(
                        "Reassign To Worker",
                        options=worker_options,
                        index=default_index,
                        format_func=lambda x: x[1],
                        key="part_details_reassign_worker_select"
                    )

                    if st.button("Send Back To Worker", key="part_details_send_back_btn"):
                        if not selected_rows:
                            st.warning("Select at least one row to send back.")
                        elif not reassign_worker or reassign_worker[0] == "":
                            st.warning("Please select a worker for reassignment.")
                        else:
                            target_worker_id = reassign_worker[0]
                            target_worker_name = reassign_worker[1]

                            update_df = read_csv(WORKER_QUOTES_PATH)
                            if update_df.empty:
                                st.error("No submissions found to update.")
                            else:
                                if "EDIT_REQUIRED" not in update_df.columns:
                                    update_df["EDIT_REQUIRED"] = "NO"
                                update_df["SUBMISSION_ID"] = update_df["SUBMISSION_ID"].astype(str).str.strip()
                                update_df["WORKER_ID"] = update_df["WORKER_ID"].astype(str).str.strip()
                                update_df["EDIT_REQUIRED"] = update_df["EDIT_REQUIRED"].fillna("").astype(str).str.strip().str.upper()
                                update_df.loc[~update_df["EDIT_REQUIRED"].isin(["YES", "NO"]), "EDIT_REQUIRED"] = "NO"

                                updated_rows = 0
                                for sub_id, quote_id, part_no, supplier in selected_rows:
                                    mask = (update_df["SUBMISSION_ID"] == str(sub_id).strip())
                                    if mask.any():
                                        update_df.loc[mask, "EDIT_REQUIRED"] = "YES"
                                        update_df.loc[mask, "WORKER_ID"] = target_worker_id
                                        updated_rows += int(mask.sum())

                                update_df.to_csv(WORKER_QUOTES_PATH, index=False, quoting=csv.QUOTE_ALL)
                                if updated_rows == 0:
                                    st.warning("No matching rows were updated.")
                                else:
                                    st.success(f"{len(selected_rows)} row(s) reassigned to {target_worker_name}.")
                                st.rerun()

            with part_details_no_quote_tab:
                no_quote_rows = part_details_view[
                    part_details_view["NO_QUOTE"] == "YES"
                ].copy()
                if no_quote_rows.empty:
                    st.info("No rows are currently marked as No Quote.")
                else:
                    display_cols = [
                        "Customer ref NO",
                        "PART NO",
                        "DESCRIPTION",
                        "WORKER_NAME",
                        "NO_QUOTE_REMARK",
                        "SUBMITTED_DATE",
                    ]
                    for col in display_cols:
                        if col not in no_quote_rows.columns:
                            no_quote_rows[col] = ""
                    no_quote_display = no_quote_rows[display_cols].copy()
                    no_quote_display = no_quote_display.rename(
                        columns={
                            "Customer ref NO": "REF NO",
                            "WORKER_NAME": "WORKER",
                            "NO_QUOTE_REMARK": "REMARK",
                            "SUBMITTED_DATE": "DATE",
                        }
                    )
                    no_quote_display["STATUS"] = "NO QUOTE"
                    st.dataframe(no_quote_display, use_container_width=True)
        else:
            st.info("No part details yet.")

    # -----------------------
    # TAB 5 - Margin & Internal Quote
    # -----------------------
    with tabs[4]:
        #st.header("Margin & Internal Quote")
        #st.subheader("Data Transfer")
        render_import_replace_csv(
            "Import Final Quotes Excel",
            FINAL_QUOTES_COLUMNS,
            FINAL_QUOTES_PATH,
            "Final quotes file replaced successfully.",
            key="import_final_quotes_margin"
        )

        worker_df = read_csv(WORKER_QUOTES_PATH)
        quotes_df = read_csv(QUOTES_PATH)

        if quotes_df.empty:
            st.info("No quotes available.")
        elif worker_df.empty:
            st.info("No part details to process.")
        else:
            quotes_df["STATUS"] = quotes_df["STATUS"].astype(str).str.strip().str.upper()
            quotes_df = quotes_df[quotes_df["STATUS"] != "DELETED"]
            if quotes_df.empty:
                st.info("No quotes available.")
                st.stop()

            for df in [worker_df, quotes_df]:
                df["QUOTE_ID"] = df["QUOTE_ID"].astype(str).str.strip()
                df["PART NO"] = df["PART NO"].astype(str).str.strip()

            if "NO_QUOTE" not in worker_df.columns:
                worker_df["NO_QUOTE"] = "NO"
            worker_df["NO_QUOTE"] = worker_df["NO_QUOTE"].fillna("").astype(str).str.strip().str.upper()
            worker_df.loc[~worker_df["NO_QUOTE"].isin(["YES", "NO"]), "NO_QUOTE"] = "NO"
            worker_df["SUPPLIER"] = worker_df["SUPPLIER"].fillna("").astype(str).str.strip()
            margin_worker_df = worker_df[
                (worker_df["NO_QUOTE"] == "NO") &
                (~worker_df["SUPPLIER"].astype(str).str.strip().str.lower().isin(["", "nan", "none"]))
            ].copy()

            customer_refs = (
                quotes_df["Customer ref NO"]
                .dropna()
                .astype(str)
                .str.strip()
                .unique()
                .tolist()
            )

            if not customer_refs:
                st.info("No customer references found in quotes.")
            else:
                sel_ref = st.selectbox("Select Customer Ref", customer_refs, key="margin_customer_ref")
                customer_quotes = quotes_df[
                    quotes_df["Customer ref NO"].astype(str).str.strip() == str(sel_ref).strip()
                ].copy()
                current_status = customer_quotes["STATUS"].astype(str).str.strip().str.upper().iloc[0]
                if current_status == "DELETED":
                    st.error("Quote is DELETED.")
                    st.stop()
                if current_status == "COMPLETED":
                    st.error("Quote is COMPLETED. Editing locked.")
                    st.stop()

                selected_quote_id = customer_quotes["QUOTE_ID"].astype(str).str.strip().iloc[0]
                total_parts = customer_quotes[
                    customer_quotes["QUOTE_ID"].astype(str).str.strip() == selected_quote_id
                ]["PART NO"].astype(str).str.strip().nunique()
                submitted_parts = margin_worker_df[
                    margin_worker_df["QUOTE_ID"].astype(str).str.strip() == selected_quote_id
                ]["PART NO"].astype(str).str.strip().nunique()

                if submitted_parts < total_parts:
                    st.warning(
                        f"Only {submitted_parts} out of {total_parts} parts have supplier submissions. "
                        "Showing available submitted parts below."
                    )

                subs_for_customer = margin_worker_df.merge(
                    customer_quotes[["QUOTE_ID", "PART NO"]].drop_duplicates(),
                    on=["QUOTE_ID", "PART NO"],
                    how="inner"
                )

                final_quotes_df = read_csv(FINAL_QUOTES_PATH)

                if subs_for_customer.empty:
                    st.info("No part details found for this customer reference.")
                else:
                    if "PRICE" in subs_for_customer.columns:
                        cost_col = "PRICE"
                    elif "COST_PRICE_EA" in subs_for_customer.columns:
                        cost_col = "COST_PRICE_EA"
                    else:
                        cost_col = "COST"

                    part_base = customer_quotes[
                        ["QUOTE_ID", "PART NO", "Customer ref NO", "DESCRIPTION", "QTY", "DUE DATE"]
                    ].drop_duplicates()
                    supplier_base = subs_for_customer.copy()
                    supplier_base["PRICE"] = pd.to_numeric(supplier_base[cost_col], errors="coerce")
                    supplier_base["SUPPLIER"] = supplier_base["SUPPLIER"].astype(str).str.strip()
                    supplier_base["CONDITION"] = supplier_base["COND_AVAILABLE"].astype(str).str.strip()
                    supplier_base["LEAD TIME"] = supplier_base["LT"].astype(str).str.strip()
                    supplier_base = supplier_base[["SUBMISSION_ID", "QUOTE_ID", "PART NO", "SUPPLIER", "SUPPLIER_COUNTRY", "PRICE", "CONDITION", "LEAD TIME"]]

                    margin_grid = part_base.merge(
                        supplier_base,
                        on=["QUOTE_ID", "PART NO"],
                        how="left"
                    )

                    if not final_quotes_df.empty:
                        if "SELECTED_SUBMISSION_ID" not in final_quotes_df.columns:
                            final_quotes_df["SELECTED_SUBMISSION_ID"] = ""
                        final_existing = final_quotes_df[[
                            "SELECTED_SUBMISSION_ID",
                            "MARGIN_PERCENT"
                        ]].copy()
                        final_existing.rename(columns={"SELECTED_SUBMISSION_ID": "SUBMISSION_ID"}, inplace=True)
                        final_existing["SUBMISSION_ID"] = final_existing["SUBMISSION_ID"].astype(str).str.strip()
                        margin_grid = margin_grid.merge(
                            final_existing,
                            on=["SUBMISSION_ID"],
                            how="left"
                        )
                    else:
                        margin_grid["MARGIN_PERCENT"] = None

                    margin_grid["PRICE"] = pd.to_numeric(margin_grid["PRICE"], errors="coerce").fillna(0.0)
                    margin_grid["QTY"] = pd.to_numeric(margin_grid["QTY"], errors="coerce").fillna(1.0)
                    margin_grid["TOTAL PRICE"] = margin_grid["PRICE"] * margin_grid["QTY"]
                    margin_grid["MARGIN_PERCENT"] = pd.to_numeric(margin_grid["MARGIN_PERCENT"], errors="coerce").fillna(15.0)
                    margin_grid["FINAL_UNIT_PRICE"] = margin_grid["PRICE"] * (1 + margin_grid["MARGIN_PERCENT"] / 100)
                    margin_grid["FINAL_TOTAL"] = margin_grid["FINAL_UNIT_PRICE"] * margin_grid["QTY"]
                    margin_grid["SELECT"] = True

                    # Rename for UI display clarity (CSV schema unchanged)
                    margin_grid = margin_grid.rename(columns={
                        "DUE DATE": "Customer Due Date",
                    })

                    st.subheader("Internal Quote Summary")
                    edit_cols = [
                        "SELECT",
                        "QUOTE_ID",
                        "Customer ref NO",
                        "PART NO",
                        "DESCRIPTION",
                        "QTY",
                        "Customer Due Date",
                        "SUPPLIER",
                        "SUPPLIER_COUNTRY",
                        "CONDITION",
                        "LEAD TIME",
                        "PRICE",
                        "TOTAL PRICE",
                        "MARGIN_PERCENT",
                        "FINAL_UNIT_PRICE",
                        "FINAL_TOTAL"
                    ]
                    assert len(edit_cols) == len(set(edit_cols)), "Duplicate column names detected in edit_cols"

                    st.caption("Select suppliers and edit margin. Final prices update from margin values.")
                    edited_margin_grid = st.data_editor(
                        margin_grid[edit_cols],
                        num_rows="fixed",
                        key="margin_grid_editor",
                        hide_index=True,
                        disabled=[
                            "QUOTE_ID",
                            "Customer ref NO",
                            "PART NO",
                            "DESCRIPTION",
                            "QTY",
                            "SUPPLIER",
                            "SUPPLIER_COUNTRY",
                            "CONDITION",
                            "LEAD TIME",
                            "PRICE",
                            "TOTAL PRICE",
                            "FINAL_UNIT_PRICE",
                            "FINAL_TOTAL",
                            "Customer Due Date"
                        ]
                    )

                    # --- Recalculate Derived Values ---
                    edited_margin_grid["PRICE"] = pd.to_numeric(
                        edited_margin_grid["PRICE"], errors="coerce"
                    ).fillna(0.0)

                    edited_margin_grid["QTY"] = pd.to_numeric(
                        edited_margin_grid["QTY"], errors="coerce"
                    ).fillna(1.0)
                    edited_margin_grid["TOTAL PRICE"] = (
                        edited_margin_grid["PRICE"] * edited_margin_grid["QTY"]
                    )

                    edited_margin_grid["MARGIN_PERCENT"] = pd.to_numeric(
                        edited_margin_grid["MARGIN_PERCENT"],
                        errors="coerce"
                    ).fillna(0.0)

                    edited_margin_grid["FINAL_UNIT_PRICE"] = (
                        edited_margin_grid["PRICE"]
                        * (1 + edited_margin_grid["MARGIN_PERCENT"] / 100)
                    )

                    edited_margin_grid["FINAL_TOTAL"] = (
                        edited_margin_grid["FINAL_UNIT_PRICE"]
                        * edited_margin_grid["QTY"]
                    )

                    st.subheader("Calculation Preview")
                    preview_columns = [
                        "Customer ref NO",
                        "PART NO",
                        "SUPPLIER",
                        "PRICE",
                        "TOTAL PRICE",
                        "MARGIN_PERCENT",
                        "FINAL_UNIT_PRICE",
                        "FINAL_TOTAL",
                    ]
                    valid_supplier_mask = ~edited_margin_grid["SUPPLIER"].astype(str).str.strip().str.lower().isin(["", "nan", "none"])
                    selected_preview = edited_margin_grid[
                        (edited_margin_grid["SELECT"] == True) & valid_supplier_mask
                    ]
                    if selected_preview.empty:
                        st.info("No suppliers selected. Nothing will be saved.")
                    else:
                        st.dataframe(
                            selected_preview[preview_columns],
                            use_container_width=True
                        )

                    if st.button("Save Draft", key="save_draft_margin"):
                        final_quotes_df = read_csv(FINAL_QUOTES_PATH)

                        if final_quotes_df.empty:
                            final_quotes_df = pd.DataFrame(columns=FINAL_QUOTES_COLUMNS)
                        else:
                            if "SUPPLIER" not in final_quotes_df.columns:
                                final_quotes_df["SUPPLIER"] = ""
                            final_quotes_df["QUOTE_ID"] = final_quotes_df["QUOTE_ID"].astype(str).str.strip()
                            final_quotes_df["PART NO"] = final_quotes_df["PART NO"].astype(str).str.strip()
                            final_quotes_df["SUPPLIER"] = final_quotes_df["SUPPLIER"].astype(str).str.strip()

                        edited_margin_grid["SELECT"] = (
                            edited_margin_grid["SELECT"]
                            .fillna(False)
                            .apply(lambda x: x if isinstance(x, bool) else str(x).strip().lower() == "true")
                        )
                        valid_supplier_mask = ~edited_margin_grid["SUPPLIER"].astype(str).str.strip().str.lower().isin(["", "nan", "none"])
                        selected_rows = edited_margin_grid[
                            (edited_margin_grid["SELECT"] == True) & valid_supplier_mask
                        ]

                        # Remove previous draft rows for this quote
                        final_quotes_df = final_quotes_df[
                            final_quotes_df["QUOTE_ID"].astype(str).str.strip() != str(selected_quote_id).strip()
                        ]

                        for idx, row in selected_rows.iterrows():
                            # Retrieve SUBMISSION_ID from the original margin_grid using index
                            internal_sub_id = str(margin_grid.loc[idx, "SUBMISSION_ID"]).strip()
                            record = {
                                "QUOTE_ID": str(row["QUOTE_ID"]).strip(),
                                "PART NO": str(row["PART NO"]).strip(),
                                "SUPPLIER": str(row["SUPPLIER"]).strip(),
                                "PRICE": float(row["PRICE"]),
                                "MARGIN_PERCENT": float(row["MARGIN_PERCENT"]),
                                "FINAL_UNIT_PRICE": float(row["FINAL_UNIT_PRICE"]),
                                "FINAL_TOTAL": float(row["FINAL_TOTAL"]),
                                "SELECTED_SUBMISSION_ID": internal_sub_id,
                                "GENERATED_DATE": datetime.now().strftime("%Y-%m-%d"),
                            }

                            final_quotes_df = pd.concat(
                                [final_quotes_df, pd.DataFrame([record])],
                                ignore_index=True
                            )

                        final_quotes_df = enforce_schema(
                            final_quotes_df,
                            FINAL_QUOTES_COLUMNS
                        )

                        final_quotes_df.to_csv(
                            FINAL_QUOTES_PATH,
                            index=False,
                            quoting=csv.QUOTE_ALL
                        )

                        st.success("Draft saved successfully.")
                        st.rerun()
                    summary_df = edited_margin_grid[
                        [
                            "Customer ref NO",
                            "PART NO",
                            "DESCRIPTION",
                            "QTY",
                            "SUPPLIER",
                            "CONDITION",
                            "LEAD TIME",
                            "PRICE",
                            "TOTAL PRICE",
                            "MARGIN_PERCENT",
                            "FINAL_UNIT_PRICE",
                            "FINAL_TOTAL",
                            "Customer Due Date"
                        ]
                    ].rename(columns={"Customer ref NO": "REF NO"})
                    safe_ref = str(sel_ref).replace(" ", "_").replace("/", "-")
                    export_data = to_excel_bytes(summary_df)
                    zip_buffer = io.BytesIO()
                    with zipfile.ZipFile(zip_buffer, "w") as zf:
                        zf.writestr("internal_quote.xlsx", export_data)

                        saved_final_df = read_csv(FINAL_QUOTES_PATH)
                        if not saved_final_df.empty:
                            saved_final_df["QUOTE_ID"] = saved_final_df["QUOTE_ID"].astype(str).str.strip()
                            saved_final_df["PART NO"] = saved_final_df["PART NO"].astype(str).str.strip()
                            if "SELECTED_SUBMISSION_ID" not in saved_final_df.columns:
                                saved_final_df["SELECTED_SUBMISSION_ID"] = ""
                            saved_final_df["SELECTED_SUBMISSION_ID"] = saved_final_df["SELECTED_SUBMISSION_ID"].astype(str).str.strip()

                            selected_quote_ids = customer_quotes["QUOTE_ID"].astype(str).str.strip().unique().tolist()
                            saved_final_df = saved_final_df[
                                saved_final_df["QUOTE_ID"].isin(selected_quote_ids)
                            ]

                            cert_df = read_csv(WORKER_QUOTES_PATH)
                            if not cert_df.empty and "CERTIFICATE_FILE" in cert_df.columns:
                                cert_df["QUOTE_ID"] = cert_df["QUOTE_ID"].astype(str).str.strip()
                                cert_df["PART NO"] = cert_df["PART NO"].astype(str).str.strip()
                                cert_df["SUBMISSION_ID"] = cert_df["SUBMISSION_ID"].astype(str).str.strip()
                                cert_df["CERTIFICATE_FILE"] = cert_df["CERTIFICATE_FILE"].fillna("").astype(str).str.strip()

                                added_files = set()
                                for _, frow in saved_final_df.iterrows():
                                    match = cert_df[
                                        (cert_df["SUBMISSION_ID"] == str(frow.get("SELECTED_SUBMISSION_ID", "")).strip())
                                    ]
                                    if match.empty:
                                        continue
                                    cert_file = str(match.iloc[0].get("CERTIFICATE_FILE", "")).strip()
                                    if not cert_file or cert_file in added_files:
                                        continue
                                    cert_path = CERTIFICATE_DIR / cert_file
                                    if cert_path.exists():
                                        zf.write(cert_path, arcname=f"certificates/{cert_file}")
                                        added_files.add(cert_file)

                    st.download_button(
                        "Download Internal Quote",
                        zip_buffer.getvalue(),
                        f"internal_quote_{safe_ref}.zip",
                        "application/zip"
                    )

    # -----------------------
    # TAB 6 - Export Client Quote
    # -----------------------
    with tabs[5]:
        #st.header("Export Client Quote")
        #st.subheader("Data Transfer")
        render_import_replace_csv(
            "Import Final Quotes Excel",
            FINAL_QUOTES_COLUMNS,
            FINAL_QUOTES_PATH,
            "Final quotes file replaced successfully.",
            key="import_final_quotes_client_export"
        )

        final_df = read_csv(FINAL_QUOTES_PATH)
        worker_df = read_csv(WORKER_QUOTES_PATH)
        quotes_df = read_csv(QUOTES_PATH)
        if not quotes_df.empty:
            quotes_df["STATUS"] = quotes_df["STATUS"].astype(str).str.strip().str.upper()
            quotes_df = quotes_df[quotes_df["STATUS"] != "DELETED"]

        if final_df.empty:
            st.info("No final quotes generated yet.")
        elif quotes_df.empty:
            st.info("No non-deleted quotes available for export.")
        else:
            # Normalize keys for reliable joins.
            for df in [final_df, worker_df, quotes_df]:
                df["QUOTE_ID"] = df["QUOTE_ID"].astype(str).str.strip()
                df["PART NO"] = df["PART NO"].astype(str).str.strip()
            if "SELECTED_SUBMISSION_ID" not in final_df.columns:
                final_df["SELECTED_SUBMISSION_ID"] = ""
            final_df["SELECTED_SUBMISSION_ID"] = final_df["SELECTED_SUBMISSION_ID"].astype(str).str.strip()
            worker_df["SUBMISSION_ID"] = worker_df["SUBMISSION_ID"].astype(str).str.strip()

            worker_mod = worker_df.rename(columns={"SUBMISSION_ID": "SELECTED_SUBMISSION_ID"})
            merged_df = final_df.merge(
                worker_mod,
                on=["SELECTED_SUBMISSION_ID", "QUOTE_ID", "PART NO"],
                how="left"
            )
            merged_df = merged_df.merge(
                quotes_df,
                on=["QUOTE_ID", "PART NO"],
                how="left"
            )

            customer_refs = sorted(
                merged_df["Customer ref NO"].dropna().astype(str).str.strip().unique().tolist()
            )
            if not customer_refs:
                st.info("No customer references found for client quote export.")
            else:
                sel_ref = st.selectbox("Select Customer Ref", customer_refs, key="client_quote_customer_ref")
                merged_df = merged_df[
                    merged_df["Customer ref NO"].astype(str).str.strip() == str(sel_ref).strip()
                ]

                if merged_df.empty:
                    st.info("No rows found for selected Customer Ref.")
                else:
                    # Initialize SELECT column
                    merged_df["SELECT"] = True
                    
                    st.subheader("Select Parts to Include in Export")
                    st.caption("Only selected parts will be included in the Excel and ZIP export.")
                    
                    # Define columns to show in the editor for admin selection
                    display_editor_cols = [
                        "SELECT",
                        "CUSTOMER ID",
                        "QUOTE_ID",
                        "PART NO",
                        "DESCRIPTION",
                        "QTY",
                        "SUPPLIER",
                        "FINAL_UNIT_PRICE",
                        "FINAL_TOTAL",
                        "COND_AVAILABLE",
                        "LT"
                    ]
                    
                    # Handle missing columns if any
                    for col in display_editor_cols:
                        if col not in merged_df.columns:
                            merged_df[col] = ""

                    edited_export_grid = st.data_editor(
                        merged_df[display_editor_cols],
                        use_container_width=True,
                        num_rows="fixed",
                        disabled=[c for c in display_editor_cols if c != "SELECT"],
                        key=f"client_export_editor_{sel_ref}"
                    )
                    
                    # Filter based on admin selection
                    selected_export_df = merged_df.loc[edited_export_grid.index[edited_export_grid["SELECT"] == True]].copy()

                    if selected_export_df.empty:
                        st.warning("No parts selected for export.")
                    else:
                        # Validate ONLY selected parts
                        missing_selection = selected_export_df[
                            selected_export_df["SELECTED_SUBMISSION_ID"].isna() |
                            (selected_export_df["SELECTED_SUBMISSION_ID"].astype(str).str.strip() == "") |
                            (selected_export_df["SELECTED_SUBMISSION_ID"].astype(str).str.strip().str.lower() == "nan")
                        ]

                        if not missing_selection.empty:
                            st.error(
                                "Some selected parts do not have a selected supplier. Please complete margin selection before exporting."
                            )
                            st.stop()

                        client_df = selected_export_df[[
                            "CUSTOMER ID",
                            "Customer ref NO",
                            "PART NO",
                            "DESCRIPTION",
                            "QTY",
                            "FINAL_UNIT_PRICE",
                            "FINAL_TOTAL",
                            "COND_AVAILABLE",
                            "LT",
                            "CERTIFICATE_TYPE",
                            "CERTIFICATE_FILE"
                        ]].rename(columns={
                            "CUSTOMER ID": "CUSTOMER ID",  
                            "Customer ref NO": "REF NO",
                            "FINAL_UNIT_PRICE": "UNIT PRICE",
                            "FINAL_TOTAL": "TOTAL PRICE",
                            "COND_AVAILABLE": "CONDITION",
                            "LT": "LEAD TIME (DAYS)",
                            "CERTIFICATE_TYPE": "CERTIFICATE TYPE"
                        })

                        st.dataframe(client_df, width='stretch')

                        client_export_df = client_df.copy()
                        export_price_cols = [
                            "PRICE",
                            "FINAL_PRICE",
                            "FINAL_TOTAL",
                            "FINAL_UNIT_PRICE",
                            "UNIT PRICE",
                            "TOTAL PRICE",
                        ]
                        for price_col in export_price_cols:
                            if price_col in client_export_df.columns:
                                client_export_df[price_col] = client_export_df[price_col].apply(
                                    lambda x: x
                                    if pd.isna(x) or str(x).strip() == ""
                                    else (str(x) if str(x).strip().startswith("$") else f"${x}")
                                )

                        excel_data = to_excel_bytes(client_export_df)
                        safe_ref = str(sel_ref).replace(" ", "_").replace("/", "-")
                        zip_buffer = io.BytesIO()
                        with zipfile.ZipFile(zip_buffer, "w") as zf:
                            zf.writestr("client_quote.xlsx", excel_data)

                            selected_quote_ids = selected_export_df["QUOTE_ID"].astype(str).str.strip().unique().tolist()
                            final_rows_for_ref = final_df[
                                final_df["QUOTE_ID"].astype(str).str.strip().isin(selected_quote_ids)
                            ].copy()
                            if not final_rows_for_ref.empty and "CERTIFICATE_FILE" in worker_df.columns:
                                worker_df["CERTIFICATE_FILE"] = worker_df["CERTIFICATE_FILE"].fillna("").astype(str).str.strip()
                                added_files = set()
                                for _, frow in final_rows_for_ref.iterrows():
                                    match = worker_df[
                                        (worker_df["SUBMISSION_ID"] == str(frow.get("SELECTED_SUBMISSION_ID", "")).strip())
                                    ]
                                    if match.empty:
                                        continue
                                    cert_file = str(match.iloc[0].get("CERTIFICATE_FILE", "")).strip()
                                    if not cert_file or cert_file in added_files:
                                        continue
                                    cert_path = CERTIFICATE_DIR / cert_file
                                    if cert_path.exists():
                                        zf.write(cert_path, arcname=f"certificates/{cert_file}")
                                        added_files.add(cert_file)

                        st.download_button(
                            "Download Client Quote ZIP",
                            zip_buffer.getvalue(),
                            f"client_quote_{safe_ref}.zip",
                            "application/zip"
                        )
                        confirm_send = st.checkbox(
                            "Confirm RFQ is finished",
                            key="confirm_send_to_client"
                        )

                        if st.button("Mark RFQ Completed", key="mark_sent_to_client"):
                            if not confirm_send:
                                st.warning("Please confirm before marking the quote as finished.")
                                st.stop()

                            quotes_df = read_csv(QUOTES_PATH)
                            ref_mask = quotes_df["Customer ref NO"].astype(str).str.strip() == str(sel_ref).strip()
                            quotes_df.loc[ref_mask, "STATUS"] = "COMPLETED"
                            quotes_df.to_csv(QUOTES_PATH, index=False, quoting=csv.QUOTE_ALL)
                            st.success("RFQ marked as COMPLETED and archived.")
                            st.rerun()


else:
    # ===========================
    # WORKER VIEW
    # ===========================

    # -----------------------
    # WORKER TAB 1 - Assigned Parts
    # -----------------------
    with tabs[0]:
        st.header("Assigned (Pending)")
        assignments_df = read_csv(ASSIGNMENTS_PATH)
        quotes_df = read_csv(QUOTES_PATH)

        # Normalize join keys to avoid dtype/whitespace mismatches.
        assignments_df["QUOTE_ID"] = assignments_df["QUOTE_ID"].astype(str).str.strip()
        assignments_df["ASSIGNED_TO"] = assignments_df["ASSIGNED_TO"].astype(str).str.strip()
        if "WORKER_DUE_DATE" not in assignments_df.columns:
            assignments_df["WORKER_DUE_DATE"] = ""
        assignments_df["WORKER_DUE_DATE"] = assignments_df["WORKER_DUE_DATE"].astype(str).str.strip()
        quotes_df["QUOTE_ID"] = quotes_df["QUOTE_ID"].astype(str).str.strip()
        quotes_df["PART NO"] = quotes_df["PART NO"].astype(str).str.strip()
        quotes_df["STATUS"] = quotes_df["STATUS"].astype(str).str.strip().str.upper()
        quotes_df = quotes_df[quotes_df["STATUS"] != "DELETED"]

        my_assignments = assignments_df[
            assignments_df["ASSIGNED_TO"].astype(str) == str(user_id)
        ].copy()
        if my_assignments.empty:
            my_assignments = pd.DataFrame(columns=["QUOTE_ID", "WORKER_DUE_DATE"])
        else:
            my_assignments["ASSIGNED_DATE"] = pd.to_datetime(my_assignments["ASSIGNED_DATE"], errors="coerce")
            my_assignments = my_assignments.sort_values(by="ASSIGNED_DATE", na_position="last")
            my_assignments = my_assignments.drop_duplicates(subset=["QUOTE_ID"], keep="last")
            my_assignments = my_assignments[["QUOTE_ID", "WORKER_DUE_DATE"]]

        my_tasks = my_assignments.merge(
            quotes_df,
            on=["QUOTE_ID"],
            how="left"
        )
        pending_tasks = my_tasks.copy()
        if my_tasks.empty:
            pending_tasks = pd.DataFrame(columns=["QUOTE_ID", "PART NO", "Customer ref NO", "DESCRIPTION", "QTY", "MEASURE_UNIT", "WORKER_DUE_DATE"])
            st.info("No parts assigned to you yet.")
        else:

            worker_submissions_df = read_csv(WORKER_QUOTES_PATH)
            if not worker_submissions_df.empty:
                worker_submissions_df = enforce_schema(worker_submissions_df, WORKER_QUOTES_COLUMNS)
                worker_submissions_df["QUOTE_ID"] = worker_submissions_df["QUOTE_ID"].astype(str).str.strip()
                worker_submissions_df["PART NO"] = worker_submissions_df["PART NO"].astype(str).str.strip()
                worker_submissions_df["WORKER_ID"] = worker_submissions_df["WORKER_ID"].astype(str).str.strip()
                worker_submissions_df["SUPPLIER"] = worker_submissions_df["SUPPLIER"].fillna("").astype(str).str.strip()
                worker_submissions_df["EDIT_REQUIRED"] = worker_submissions_df["EDIT_REQUIRED"].fillna("").astype(str).str.strip().str.upper()
                worker_submissions_df["NO_QUOTE"] = worker_submissions_df["NO_QUOTE"].fillna("").astype(str).str.strip().str.upper()
                worker_submissions_df.loc[~worker_submissions_df["EDIT_REQUIRED"].isin(["YES", "NO"]), "EDIT_REQUIRED"] = "NO"
                worker_submissions_df.loc[~worker_submissions_df["NO_QUOTE"].isin(["YES", "NO"]), "NO_QUOTE"] = "NO"

                worker_submissions_df = worker_submissions_df[
                    worker_submissions_df["WORKER_ID"] == str(user_id).strip()
                ].copy()

                has_supplier_quote = (
                    (worker_submissions_df["NO_QUOTE"] == "NO") &
                    (~worker_submissions_df["SUPPLIER"].astype(str).str.strip().str.lower().isin(["", "nan", "none"]))
                )
                # Allow multiple supplier submissions. Exclude if NO_QUOTE == "YES" or EDIT_REQUIRED == "YES".
                completed_mask = (
                    (worker_submissions_df["NO_QUOTE"] == "YES") |
                    (worker_submissions_df["EDIT_REQUIRED"] == "YES")
                )
                completed_keys = set(
                    zip(
                        worker_submissions_df.loc[completed_mask, "QUOTE_ID"].astype(str).str.strip(),
                        worker_submissions_df.loc[completed_mask, "PART NO"].astype(str).str.strip(),
                    )
                )
                if completed_keys:
                    pending_tasks = my_tasks[
                        ~my_tasks.apply(
                            lambda row: (
                                clean_text(row["QUOTE_ID"]),
                                clean_text(row["PART NO"])
                            ) in completed_keys,
                            axis=1
                        )
                    ].copy()

            if "MEASURE_UNIT" not in pending_tasks.columns:
                pending_tasks["MEASURE_UNIT"] = "EA"

            display_columns = [
                "QUOTE_ID",
                "Customer ref NO",
                "PART NO",
                "DESCRIPTION",
                "QTY",
                "MEASURE_UNIT",
                "WORKER_DUE_DATE"
            ]
            if pending_tasks.empty:
                st.info("No pending parts in your work queue.")
            else:
                assigned_parts_view = pending_tasks[display_columns]
                filtered_assigned_parts_view = apply_assigned_parts_filters(
                    assigned_parts_view,
                    "worker_assigned_parts"
                )
                st.dataframe(filtered_assigned_parts_view, width='stretch')
                render_export_button(
                    "Export Assigned Parts Excel",
                    filtered_assigned_parts_view,
                    f"assigned_parts_{username}.xlsx"
                )

    # -----------------------
    # WORKER TAB 2 - Submit Supplier Info
    # -----------------------
    with tabs[1]:
        st.header("Submit Supplier Info")
        if not pending_tasks.empty:
            pending_tasks_export = pending_tasks[["QUOTE_ID", "PART NO", "Customer ref NO", "DESCRIPTION", "QTY", "WORKER_DUE_DATE"]].copy()
            render_export_button(
                "Export My Pending Assigned Tasks Excel",
                pending_tasks_export,
                f"my_pending_tasks_{username}.xlsx"
            )

        if not pending_tasks.empty:
            my_tasks_norm = pending_tasks.copy()
            for col in ["QUOTE_ID", "PART NO", "DESCRIPTION", "Customer ref NO"]:
                if col not in my_tasks_norm.columns:
                    my_tasks_norm[col] = ""
                my_tasks_norm[col] = my_tasks_norm[col].fillna("").astype(str).str.strip()
            task_option_map = {}
            for _, row in my_tasks_norm.iterrows():
                base_label = f"{row['PART NO']} | {row['DESCRIPTION']}"
                label = make_unique_label(
                    base_label,
                    [row["QUOTE_ID"], row["Customer ref NO"]],
                    set(task_option_map.keys())
                )
                task_option_map[label] = (row["QUOTE_ID"], row["PART NO"])

            selected_task_display = st.selectbox(
                "Select Assigned Task",
                list(task_option_map.keys())
            )
            sel_quote_id, sel_part_no = task_option_map[selected_task_display]
            selected_key = (str(sel_quote_id).strip(), str(sel_part_no).strip())

            valid_keys_from_quotes = set()
            if not quotes_df.empty:
                valid_keys_from_quotes = {
                    (clean_text(row["QUOTE_ID"]), clean_text(row["PART NO"]))
                    for _, row in quotes_df[["QUOTE_ID", "PART NO"]].drop_duplicates().iterrows()
                }
            valid_keys_from_tasks = {
                (clean_text(row["QUOTE_ID"]), clean_text(row["PART NO"]))
                for _, row in my_tasks_norm[["QUOTE_ID", "PART NO"]].drop_duplicates().iterrows()
            }

            # Guard: Block submissions if RFQ is COMPLETED
            if not quotes_df.empty:
                q_mask_comp = quotes_df["QUOTE_ID"].astype(str).str.strip() == str(sel_quote_id).strip()
                if any(q_mask_comp):
                    q_status_comp = quotes_df[q_mask_comp]["STATUS"].astype(str).str.strip().str.upper().iloc[0]
                    if q_status_comp == "COMPLETED":
                        st.warning("This RFQ is completed and no longer accepts submissions.")
                        st.stop()

            with st.expander("Mark as No Quote"):
                no_quote_remark = st.text_area(
                    "Reason for No Quote",
                    key=f"no_quote_reason_{safe_filename_part(sel_quote_id)}_{safe_filename_part(sel_part_no)}",
                    help="Explain why no supplier quote could be obtained."
                )
                if st.button("Confirm No Quote", key=f"mark_no_quote_{safe_filename_part(sel_quote_id)}_{safe_filename_part(sel_part_no)}"):
                    if selected_key not in valid_keys_from_quotes or selected_key not in valid_keys_from_tasks:
                        st.error("Invalid part selection detected.")
                        st.stop()
                    no_quote_remark = no_quote_remark.strip()
                    if not no_quote_remark:
                        st.error("Reason for No Quote is required.")
                        st.stop()

                    no_quote_df = read_csv(WORKER_QUOTES_PATH)
                    no_quote_df = enforce_schema(no_quote_df, WORKER_QUOTES_COLUMNS)
                    no_quote_df["QUOTE_ID"] = no_quote_df["QUOTE_ID"].astype(str).str.strip()
                    no_quote_df["PART NO"] = no_quote_df["PART NO"].astype(str).str.strip()
                    no_quote_df["SUPPLIER"] = no_quote_df["SUPPLIER"].fillna("").astype(str).str.strip()
                    no_quote_df["WORKER_ID"] = no_quote_df["WORKER_ID"].astype(str).str.strip()

                    existing_ids = no_quote_df["SUBMISSION_ID"].dropna().astype(str)
                    valid_ids = [int(x.replace("SUB", "")) for x in existing_ids if x.startswith("SUB") and x.replace("SUB", "").isdigit()]
                    next_id = max(valid_ids) + 1 if valid_ids else 1
                    submission_id = f"SUB{next_id:04d}"
                    
                    no_quote_row = {
                        "SUBMISSION_ID": submission_id,
                        "QUOTE_ID": sel_quote_id,
                        "PART NO": sel_part_no,
                        "SUPPLIER": "",
                        "SUPPLIER_COUNTRY": "",
                        "SUPPLIER_SOURCE": "",
                        "PRICE": None,
                        "COND_AVAILABLE": "",
                        "QTY_AVAILABLE": None,
                        "LT": "",
                        "CERTIFICATE_AVAILABLE": "NO",
                        "CERTIFICATE_FILE": "",
                        "CERTIFICATE_TYPE": "",
                        "REMARKS": "NO QUOTE",
                        "WORKER_ID": user_id,
                        "SUBMITTED_DATE": datetime.now().strftime("%Y-%m-%d"),
                        "EDIT_REQUIRED": "NO",
                        "NO_QUOTE": "YES",
                        "NO_QUOTE_REMARK": no_quote_remark,
                    }

                    no_quote_mask = (
                        (no_quote_df["QUOTE_ID"] == str(sel_quote_id).strip()) &
                        (no_quote_df["PART NO"] == str(sel_part_no).strip()) &
                        (no_quote_df["SUPPLIER"] == "") &
                        (no_quote_df["WORKER_ID"] == str(user_id).strip())
                    )

                    if no_quote_mask.any():
                        for col, value in no_quote_row.items():
                            no_quote_df.loc[no_quote_mask, col] = value
                    else:
                        no_quote_df = pd.concat([no_quote_df, pd.DataFrame([no_quote_row])], ignore_index=True)
                        no_quote_df = enforce_schema(no_quote_df, WORKER_QUOTES_COLUMNS)

                    no_quote_df.to_csv(WORKER_QUOTES_PATH, index=False, quoting=csv.QUOTE_ALL)
                    st.success("Part marked as No Quote.")
                    st.rerun()

            with st.form("submission_form"):
                col1, col2 = st.columns(2)
                with col1:
                    cost_price = st.number_input("PRICE", min_value=0.0, step=0.01)
                    cond_available = st.selectbox(
                        "COND AVAILABLE",
                        ["NE", "NS", "OH", "SV", "AR","FN", "MOD","RP","IN"]
                    )
                    qty_available = st.number_input("QTY AVAILABLE", min_value=0, step=1)
                with col2:
                    supplier = st.text_input("SUPPLIER")
                    supplier_country = st.text_input("SUPPLIER COUNTRY")
                    supplier_source = st.text_input("SUPPLIER SOURCE")
                    lt = st.text_input("LT (Lead Time)")
                    certificate_available = st.toggle("Certificate Available")
                    certificate_type = st.text_input("Certificate Type", key=f"cert_type_{user_id}_{sel_quote_id}_{sel_part_no}")
                    uploaded_cert = None
                    if certificate_available:
                        uploaded_cert = st.file_uploader(
                            "Upload Certificate (PDF only)",
                            type=["pdf"],
                            key=f"cert_upload_{user_id}_{sel_quote_id}_{sel_part_no}"
                        )
                    remarks = st.text_area("REMARKS")

                submitted = st.form_submit_button("Submit Supplier Quote")

                if submitted:
                    if selected_key not in valid_keys_from_quotes or selected_key not in valid_keys_from_tasks:
                        st.error("Invalid part selection detected.")
                        st.stop()

                    supplier = supplier.strip()
                    if not supplier or cost_price <= 0:
                        st.error("Supplier and Cost Price are required.")
                    elif certificate_available and uploaded_cert is None:
                        st.error("Please upload certificate file.")
                        st.stop()
                    else:
                        full_df_for_id = read_csv(WORKER_QUOTES_PATH)
                        existing_ids = full_df_for_id["SUBMISSION_ID"].dropna().astype(str)
                        valid_ids = [int(x.replace("SUB", "")) for x in existing_ids if x.startswith("SUB") and x.replace("SUB", "").isdigit()]
                        next_id = max(valid_ids) + 1 if valid_ids else 1
                        submission_id = f"SUB{next_id:04d}"
                        
                        new_submission = {
                            "SUBMISSION_ID": submission_id,
                            "QUOTE_ID": sel_quote_id,
                            "PART NO": sel_part_no,
                            "SUPPLIER": supplier,
                            "SUPPLIER_COUNTRY": supplier_country,
                            "SUPPLIER_SOURCE": supplier_source,
                            "PRICE": cost_price,
                            "COND_AVAILABLE": cond_available,
                            "QTY_AVAILABLE": qty_available,
                            "LT": lt,
                            "CERTIFICATE_AVAILABLE": "YES" if certificate_available else "NO",
                            "CERTIFICATE_FILE": None,
                            "CERTIFICATE_TYPE": certificate_type if certificate_available else "",
                            "REMARKS": remarks,
                            "WORKER_ID": user_id,
                            "SUBMITTED_DATE": datetime.now().strftime("%Y-%m-%d"),
                            "EDIT_REQUIRED": "NO",
                            "NO_QUOTE": "NO",
                            "NO_QUOTE_REMARK": "",
                        }

                        if certificate_available and uploaded_cert is not None:
                            safe_quote_id = safe_filename_part(sel_quote_id)
                            safe_part_no = safe_filename_part(sel_part_no)
                            safe_supplier = safe_filename_part(supplier)
                            cert_filename = f"{safe_quote_id}_{safe_part_no}_{safe_supplier}.pdf"
                            cert_path = CERTIFICATE_DIR / cert_filename
                            cert_path.write_bytes(uploaded_cert.getbuffer())
                            new_submission["CERTIFICATE_FILE"] = cert_filename

                        worker_existing_df = read_csv(WORKER_QUOTES_PATH)
                        if worker_existing_df.empty:
                            existing_mask = pd.Series(dtype=bool)
                        else:
                            worker_existing_df = enforce_schema(worker_existing_df, WORKER_QUOTES_COLUMNS)
                            worker_existing_df["QUOTE_ID"] = worker_existing_df["QUOTE_ID"].astype(str).str.strip()
                            worker_existing_df["PART NO"] = worker_existing_df["PART NO"].astype(str).str.strip()
                            worker_existing_df["SUPPLIER"] = worker_existing_df["SUPPLIER"].astype(str).str.strip()
                            existing_mask = (
                                (worker_existing_df["QUOTE_ID"] == str(sel_quote_id).strip()) &
                                (worker_existing_df["PART NO"] == str(sel_part_no).strip()) &
                                (worker_existing_df["SUPPLIER"] == str(supplier).strip())
                            )

                        if not worker_existing_df.empty and existing_mask.any():
                            # Retain existing SUBMISSION_ID
                            existing_sub_id = worker_existing_df.loc[existing_mask, "SUBMISSION_ID"].values[0]
                            if str(existing_sub_id).strip():
                                new_submission["SUBMISSION_ID"] = existing_sub_id
                                
                            for col, value in new_submission.items():
                                worker_existing_df.loc[existing_mask, col] = value
                            worker_existing_df.to_csv(WORKER_QUOTES_PATH, index=False, quoting=csv.QUOTE_ALL)
                            st.success("Existing supplier quotation updated successfully.")
                        else:
                            append_to_csv(WORKER_QUOTES_PATH, new_submission, WORKER_QUOTES_COLUMNS)
                            st.success("Supplier quotation submitted successfully.")

                        status_changed = update_quote_status_if_fully_submitted(sel_quote_id)
                        if status_changed:
                            st.success("All parts submitted. Quote status updated to SUBMITTED.")
                        st.rerun()
        else:
            st.info("No pending parts in your work queue.")

    # -----------------------
    # WORKER TAB 3 - My Submissions
    # -----------------------
    with tabs[2]:
        st.header("My Submitted Quotes")

        worker_df = read_csv(WORKER_QUOTES_PATH)
        quotes_df = read_csv(QUOTES_PATH)

        if worker_df.empty:
            st.info("No submissions yet.")
        else:
            worker_df = enforce_schema(worker_df, WORKER_QUOTES_COLUMNS)
            worker_df["QUOTE_ID"] = worker_df["QUOTE_ID"].astype(str).str.strip()
            worker_df["PART NO"] = worker_df["PART NO"].astype(str).str.strip()
            worker_df["SUPPLIER"] = worker_df["SUPPLIER"].fillna("").astype(str).str.strip()
            worker_df["WORKER_ID"] = worker_df["WORKER_ID"].astype(str).str.strip()
            worker_df["EDIT_REQUIRED"] = worker_df["EDIT_REQUIRED"].fillna("").astype(str).str.strip().str.upper()
            worker_df["NO_QUOTE"] = worker_df["NO_QUOTE"].fillna("").astype(str).str.strip().str.upper()
            worker_df.loc[~worker_df["EDIT_REQUIRED"].isin(["YES", "NO"]), "EDIT_REQUIRED"] = "NO"
            worker_df.loc[~worker_df["NO_QUOTE"].isin(["YES", "NO"]), "NO_QUOTE"] = "NO"

            if quotes_df.empty:
                quote_meta = pd.DataFrame(columns=["QUOTE_ID", "PART NO", "Customer ref NO", "DESCRIPTION", "STATUS"])
            else:
                quotes_df["QUOTE_ID"] = quotes_df["QUOTE_ID"].astype(str).str.strip()
                quotes_df["PART NO"] = quotes_df["PART NO"].astype(str).str.strip()
                quotes_df["STATUS"] = quotes_df["STATUS"].astype(str).str.strip().str.upper()
                quote_meta = (
                    quotes_df.sort_values("DATE")
                    [["QUOTE_ID", "PART NO", "Customer ref NO", "DESCRIPTION", "STATUS"]]
                    .drop_duplicates(subset=["QUOTE_ID", "PART NO"], keep="last")
                )

            my_quotes = worker_df[
                worker_df["WORKER_ID"].astype(str).str.strip() == str(user_id)
            ].copy()

            if my_quotes.empty:
                st.info("No submissions yet.")
            else:
                merged = my_quotes.merge(
                    quote_meta,
                    on=["QUOTE_ID", "PART NO"],
                    how="left"
                )
                if "STATUS" not in merged.columns:
                    merged["STATUS"] = ""
                merged = merged[merged["STATUS"].astype(str).str.strip().str.upper() != "DELETED"]

                submitted_subtab, reassigned_subtab, no_quote_subtab = st.tabs(["Submitted", "Re-Assigned", "No Quote"])

                with submitted_subtab:
                    submitted_supplier_mask = ~merged["SUPPLIER"].astype(str).str.strip().str.lower().isin(["", "nan", "none"])
                    submitted_merged = merged[
                        (merged["NO_QUOTE"] == "NO") &
                        (merged["EDIT_REQUIRED"] == "NO") &
                        submitted_supplier_mask
                    ].copy()

                    # Final safety guard
                    submitted_merged = submitted_merged.drop_duplicates(subset=["QUOTE_ID", "PART NO", "SUPPLIER"])

                    if submitted_merged.empty:
                        st.info("No submitted supplier quotes yet.")
                    else:
                        display_cols = [
                            "Customer ref NO",
                            "PART NO",
                            "DESCRIPTION",
                            "SUPPLIER",
                            "SUPPLIER_COUNTRY",
                            "SUPPLIER_SOURCE",
                            "PRICE",
                            "COND_AVAILABLE",
                            "QTY_AVAILABLE",
                            "LT",
                            "CERTIFICATE_AVAILABLE",
                            "CERTIFICATE_TYPE",
                            "SUBMITTED_DATE",
                        ]

                        my_submissions_view = submitted_merged[display_cols].copy()
                        # Keep filter helper compatibility without changing displayed columns.
                        my_submissions_view["REF NO"] = my_submissions_view["Customer ref NO"]
                        my_submissions_view["CONDITION"] = my_submissions_view["COND_AVAILABLE"]
                        my_submissions_view["SUBMITTED DATE"] = my_submissions_view["SUBMITTED_DATE"]

                        filtered_my_submissions_view = apply_my_submissions_filters(
                            my_submissions_view,
                            "worker_my_submissions"
                        )
                        display_filtered_my_submissions = filtered_my_submissions_view[display_cols].copy()
                        st.dataframe(display_filtered_my_submissions, width='stretch')
                        render_export_button(
                            "Export My Submissions Excel",
                            display_filtered_my_submissions,
                            f"my_submissions_{username}.xlsx"
                        )

                        submitted_quotes_only = my_quotes[
                            (my_quotes["NO_QUOTE"] == "NO") &
                            (my_quotes["EDIT_REQUIRED"] == "NO") &
                            (~my_quotes["SUPPLIER"].astype(str).str.strip().str.lower().isin(["", "nan", "none"]))
                        ].copy()

                        if not submitted_quotes_only.empty:
                            st.divider()
                            st.subheader("Attach / Update Certificate")

                            attach_option_map = {}
                            for _, row in submitted_quotes_only.iterrows():
                                base_label = f"{row['PART NO']} | {row['SUPPLIER']}"
                                label = make_unique_label(
                                    base_label,
                                    [row["QUOTE_ID"]],
                                    set(attach_option_map.keys())
                                )
                                attach_option_map[label] = (
                                    str(row.get("SUBMISSION_ID", "")).strip(),
                                    clean_text(row["QUOTE_ID"]),
                                    clean_text(row["PART NO"]),
                                    clean_text(row["SUPPLIER"]),
                                )

                            selected_submission = st.selectbox(
                                "Select Submission",
                                list(attach_option_map.keys()),
                                key="attach_cert_select"
                            )
                            cert_type_attach = st.text_input(
                                "Certificate Type (optional)",
                                key="attach_cert_type"
                            )
                            uploaded_cert_attach = st.file_uploader(
                                "Upload Certificate (PDF)",
                                type=["pdf"],
                                key="attach_cert_uploader"
                            )

                            if st.button("Upload Certificate", key="attach_cert_btn"):
                                if uploaded_cert_attach is None:
                                    st.error("Please select a certificate file to upload.")
                                else:
                                    att_sub_id, att_quote_id, att_part_no, att_supplier = attach_option_map[selected_submission]

                                    cert_filename = (
                                        f"{safe_filename_part(att_quote_id)}_"
                                        f"{safe_filename_part(att_part_no)}_"
                                        f"{safe_filename_part(att_supplier)}.pdf"
                                    )
                                    cert_path = CERTIFICATE_DIR / cert_filename
                                    cert_path.write_bytes(uploaded_cert_attach.getbuffer())

                                    wdf = read_csv(WORKER_QUOTES_PATH)
                                    wdf = enforce_schema(wdf, WORKER_QUOTES_COLUMNS)
                                    wdf["QUOTE_ID"] = wdf["QUOTE_ID"].astype(str).str.strip()
                                    wdf["PART NO"] = wdf["PART NO"].astype(str).str.strip()
                                    wdf["SUPPLIER"] = wdf["SUPPLIER"].fillna("").astype(str).str.strip()

                                    mask = (wdf["SUBMISSION_ID"] == att_sub_id)
                                    wdf.loc[mask, "CERTIFICATE_FILE"] = cert_filename
                                    wdf.loc[mask, "CERTIFICATE_AVAILABLE"] = "YES"
                                    if cert_type_attach.strip():
                                        wdf.loc[mask, "CERTIFICATE_TYPE"] = cert_type_attach.strip()

                                    wdf.to_csv(WORKER_QUOTES_PATH, index=False, quoting=csv.QUOTE_ALL)
                                    st.success("Certificate attached successfully.")
                                    st.rerun()

                with reassigned_subtab:
                    reassigned_view = merged[
                        (merged["EDIT_REQUIRED"] == "YES") &
                        (merged["NO_QUOTE"] != "YES")
                    ].copy()
                    if reassigned_view.empty:
                        st.info("No rows were sent back for correction.")
                    else:
                        st.dataframe(
                            reassigned_view[
                                [
                                    "QUOTE_ID",
                                    "Customer ref NO",
                                    "PART NO",
                                    "DESCRIPTION",
                                    "SUPPLIER",
                                    "PRICE",
                                    "COND_AVAILABLE",
                                    "QTY_AVAILABLE",
                                    "LT",
                                    "REMARKS",
                                ]
                            ],
                            width='stretch'
                        )

                        key_rows = reassigned_view[["SUBMISSION_ID", "QUOTE_ID", "PART NO", "SUPPLIER"]].drop_duplicates()
                        row_options = list(key_rows.itertuples(index=False, name=None))
                        selected_key = st.selectbox(
                            "Select Re-Assigned Row",
                            row_options,
                            format_func=lambda x: f"{x[1]} | {x[2]} | {x[3]}",
                            key="worker_reassigned_select",
                        )

                        selected_row = reassigned_view[
                            reassigned_view["SUBMISSION_ID"] == str(selected_key[0]).strip()
                        ].iloc[-1]

                        current_price = pd.to_numeric(selected_row.get("PRICE", 0), errors="coerce")
                        if pd.isna(current_price):
                            current_price = 0.0
                        current_qty = pd.to_numeric(selected_row.get("QTY_AVAILABLE", 0), errors="coerce")
                        if pd.isna(current_qty):
                            current_qty = 0
                        cond_options = ["NE", "NS", "OH", "SV", "AR", "FN", "MOD","RP","IN"]
                        current_cond = str(selected_row.get("COND_AVAILABLE", "")).strip()
                        cond_index = cond_options.index(current_cond) if current_cond in cond_options else 0
                        current_cert_available = str(selected_row.get("CERTIFICATE_AVAILABLE", "")).strip().upper() == "YES"

                        with st.form("reassigned_update_form"):
                            c1, c2 = st.columns(2)
                            with c1:
                                st.text_input("QUOTE_ID", value=str(selected_key[1]), disabled=True)
                                st.text_input("PART NO", value=str(selected_key[2]), disabled=True)
                                supplier_display = st.text_input("SUPPLIER", value=str(selected_key[3]), disabled=True)
                                price = st.number_input("PRICE", min_value=0.0, step=0.01, value=float(current_price))
                                cond_available = st.selectbox("COND AVAILABLE", cond_options, index=cond_index)
                                qty_available = st.number_input("QTY AVAILABLE", min_value=0, step=1, value=int(current_qty))
                            with c2:
                                supplier_country = st.text_input(
                                    "SUPPLIER COUNTRY",
                                    value=str(selected_row.get("SUPPLIER_COUNTRY", "")).strip()
                                )
                                supplier_source = st.text_input(
                                    "SUPPLIER SOURCE",
                                    value=str(selected_row.get("SUPPLIER_SOURCE", "")).strip()
                                )
                                lt = st.text_input("LT (Lead Time)", value=str(selected_row.get("LT", "")).strip())
                                certificate_available = st.toggle("Certificate Available", value=current_cert_available)
                                certificate_type = st.text_input(
                                    "Certificate Type",
                                    value=str(selected_row.get("CERTIFICATE_TYPE", "")).strip(),
                                    key=f"reassign_cert_type_{selected_key[1]}_{selected_key[2]}_{selected_key[3]}"
                                )
                                uploaded_cert = None
                                if certificate_available:
                                    uploaded_cert = st.file_uploader(
                                        "Upload Certificate (PDF only)",
                                        type=["pdf"],
                                        key=f"reassign_cert_upload_{selected_key[1]}_{selected_key[2]}_{selected_key[3]}"
                                    )
                                remarks = st.text_area(
                                    "REMARKS",
                                    value=str(selected_row.get("REMARKS", "")).strip()
                                )

                            update_submission = st.form_submit_button("Update Submission")

                            if update_submission:
                                if price <= 0:
                                    st.error("Price must be greater than zero.")
                                else:
                                    cert_file = str(selected_row.get("CERTIFICATE_FILE", "")).strip()
                                    cert_type_value = certificate_type.strip() if certificate_available else ""

                                    if certificate_available and uploaded_cert is not None:
                                        cert_filename = (
                                            f"{safe_filename_part(selected_key[1])}_"
                                            f"{safe_filename_part(selected_key[2])}_"
                                            f"{safe_filename_part(supplier_display)}.pdf"
                                        )
                                        cert_path = CERTIFICATE_DIR / cert_filename
                                        cert_path.write_bytes(uploaded_cert.getbuffer())
                                        cert_file = cert_filename
                                    elif not certificate_available:
                                        cert_file = ""

                                    update_df = read_csv(WORKER_QUOTES_PATH)
                                    update_df = enforce_schema(update_df, WORKER_QUOTES_COLUMNS)
                                    update_df["QUOTE_ID"] = update_df["QUOTE_ID"].astype(str).str.strip()
                                    update_df["PART NO"] = update_df["PART NO"].astype(str).str.strip()
                                    update_df["SUPPLIER"] = update_df["SUPPLIER"].fillna("").astype(str).str.strip()

                                    mask = (update_df["SUBMISSION_ID"] == str(selected_key[0]).strip())

                                    if not mask.any():
                                        st.error("Submission row not found.")
                                    else:
                                        update_df.loc[mask, "SUPPLIER_COUNTRY"] = supplier_country
                                        update_df.loc[mask, "SUPPLIER_SOURCE"] = supplier_source
                                        update_df.loc[mask, "PRICE"] = price
                                        update_df.loc[mask, "COND_AVAILABLE"] = cond_available
                                        update_df.loc[mask, "QTY_AVAILABLE"] = qty_available
                                        update_df.loc[mask, "LT"] = lt
                                        update_df.loc[mask, "REMARKS"] = remarks
                                        update_df.loc[mask, "CERTIFICATE_AVAILABLE"] = "YES" if certificate_available else "NO"
                                        update_df.loc[mask, "CERTIFICATE_FILE"] = cert_file
                                        update_df.loc[mask, "CERTIFICATE_TYPE"] = cert_type_value
                                        update_df.loc[mask, "SUBMITTED_DATE"] = datetime.now().strftime("%Y-%m-%d")
                                        update_df.loc[mask, "EDIT_REQUIRED"] = "NO"
                                        update_df.loc[mask, "NO_QUOTE"] = "NO"
                                        update_df.to_csv(WORKER_QUOTES_PATH, index=False, quoting=csv.QUOTE_ALL)

                                        st.success("Submission updated successfully.")
                                        st.rerun()

                with no_quote_subtab:
                    no_quote_view = merged[merged["NO_QUOTE"] == "YES"].copy()
                    if no_quote_view.empty:
                        st.info("No parts marked as No Quote.")
                    else:
                        if "NO_QUOTE_REMARK" not in no_quote_view.columns:
                            no_quote_view["NO_QUOTE_REMARK"] = ""
                        no_quote_display = no_quote_view[
                            ["Customer ref NO", "PART NO", "DESCRIPTION", "NO_QUOTE_REMARK"]
                        ].rename(
                            columns={
                                "Customer ref NO": "REF NO",
                                "NO_QUOTE_REMARK": "REMARK",
                            }
                        ).copy()
                        no_quote_display["STATUS"] = "NO QUOTE"
                        st.dataframe(no_quote_display, width='stretch')

                        no_quote_key_rows = no_quote_view[["SUBMISSION_ID", "QUOTE_ID", "PART NO", "DESCRIPTION"]].drop_duplicates()
                        no_quote_options = list(no_quote_key_rows.itertuples(index=False, name=None))
                        selected_no_quote_key = st.selectbox(
                            "Select No Quote Part",
                            no_quote_options,
                            format_func=lambda x: f"{x[2]} | {x[3]} | {x[1]}",
                            key="no_quote_select_row"
                        )

                        with st.form("add_supplier_from_no_quote_form"):
                            c1, c2 = st.columns(2)
                            with c1:
                                st.text_input("QUOTE_ID", value=str(selected_no_quote_key[1]), disabled=True)
                                st.text_input("PART NO", value=str(selected_no_quote_key[2]), disabled=True)
                                supplier = st.text_input("SUPPLIER", key="no_quote_supplier")
                                price = st.number_input("PRICE", min_value=0.0, step=0.01, key="no_quote_price")
                                cond_available = st.selectbox(
                                    "COND AVAILABLE",
                                    ["NE", "NS", "OH", "SV", "AR", "FN", "MOD", "RP", "IN"],
                                    key="no_quote_cond"
                                )
                                qty_available = st.number_input("QTY AVAILABLE", min_value=0, step=1, key="no_quote_qty")
                            with c2:
                                supplier_country = st.text_input("SUPPLIER COUNTRY", key="no_quote_country")
                                supplier_source = st.text_input("SUPPLIER SOURCE", key="no_quote_source")
                                lt = st.text_input("LT (Lead Time)", key="no_quote_lt")
                                certificate_available = st.toggle("Certificate Available", key="no_quote_cert_available")
                                certificate_type = st.text_input("Certificate Type", key="no_quote_cert_type")
                                uploaded_cert = None
                                if certificate_available:
                                    uploaded_cert = st.file_uploader(
                                        "Upload Certificate (PDF only)",
                                        type=["pdf"],
                                        key="no_quote_cert_upload"
                                    )
                                remarks = st.text_area("REMARKS", key="no_quote_remarks")

                            add_supplier = st.form_submit_button("Add Supplier Quote")

                            if add_supplier:
                                supplier = supplier.strip()
                                if not supplier or price <= 0:
                                    st.error("Supplier and Price are required.")
                                elif certificate_available and uploaded_cert is None:
                                    st.error("Please upload certificate file.")
                                else:
                                    selected_sub_id = str(selected_no_quote_key[0]).strip()
                                    selected_quote_id = str(selected_no_quote_key[1]).strip()
                                    selected_part_no = str(selected_no_quote_key[2]).strip()

                                    cert_file = ""
                                    if certificate_available and uploaded_cert is not None:
                                        cert_file = (
                                            f"{safe_filename_part(selected_quote_id)}_"
                                            f"{safe_filename_part(selected_part_no)}_"
                                            f"{safe_filename_part(supplier)}.pdf"
                                        )
                                        cert_path = CERTIFICATE_DIR / cert_file
                                        cert_path.write_bytes(uploaded_cert.getbuffer())

                                    update_df = read_csv(WORKER_QUOTES_PATH)
                                    update_df = enforce_schema(update_df, WORKER_QUOTES_COLUMNS)
                                    update_df["QUOTE_ID"] = update_df["QUOTE_ID"].astype(str).str.strip()
                                    update_df["PART NO"] = update_df["PART NO"].astype(str).str.strip()
                                    update_df["SUPPLIER"] = update_df["SUPPLIER"].fillna("").astype(str).str.strip()
                                    update_df["WORKER_ID"] = update_df["WORKER_ID"].astype(str).str.strip()

                                    no_quote_mask = (update_df["SUBMISSION_ID"] == selected_sub_id) & (update_df["SUPPLIER"] == "")
                                    supplier_mask = (
                                        (update_df["QUOTE_ID"] == selected_quote_id) &
                                        (update_df["PART NO"] == selected_part_no) &
                                        (update_df["SUPPLIER"] == supplier)
                                    )

                                    existing_ids = update_df["SUBMISSION_ID"].dropna().astype(str)
                                    valid_ids = [int(x.replace("SUB", "")) for x in existing_ids if x.startswith("SUB") and x.replace("SUB", "").isdigit()]
                                    next_id = max(valid_ids) + 1 if valid_ids else 1
                                    submission_id = f"SUB{next_id:04d}"
                                    
                                    row_data = {
                                        "SUBMISSION_ID": submission_id,
                                        "QUOTE_ID": selected_quote_id,
                                        "PART NO": selected_part_no,
                                        "SUPPLIER": supplier,
                                        "SUPPLIER_COUNTRY": supplier_country,
                                        "SUPPLIER_SOURCE": supplier_source,
                                        "PRICE": price,
                                        "COND_AVAILABLE": cond_available,
                                        "QTY_AVAILABLE": qty_available,
                                        "LT": lt,
                                        "CERTIFICATE_AVAILABLE": "YES" if certificate_available else "NO",
                                        "CERTIFICATE_FILE": cert_file,
                                        "CERTIFICATE_TYPE": certificate_type if certificate_available else "",
                                        "REMARKS": remarks,
                                        "WORKER_ID": user_id,
                                        "SUBMITTED_DATE": datetime.now().strftime("%Y-%m-%d"),
                                        "EDIT_REQUIRED": "NO",
                                        "NO_QUOTE": "NO",
                                        "NO_QUOTE_REMARK": "",
                                    }

                                    if no_quote_mask.any():
                                        # Retain original SUBMISSION_ID for overwrites
                                        row_data["SUBMISSION_ID"] = selected_sub_id
                                        for col, value in row_data.items():
                                            update_df.loc[no_quote_mask, col] = value
                                    elif supplier_mask.any():
                                        existing_sub_id = update_df.loc[supplier_mask, "SUBMISSION_ID"].values[0]
                                        if str(existing_sub_id).strip():
                                            row_data["SUBMISSION_ID"] = existing_sub_id
                                        for col, value in row_data.items():
                                            update_df.loc[supplier_mask, col] = value
                                    else:
                                        update_df = pd.concat([update_df, pd.DataFrame([row_data])], ignore_index=True)
                                        update_df = enforce_schema(update_df, WORKER_QUOTES_COLUMNS)

                                    update_df.to_csv(WORKER_QUOTES_PATH, index=False, quoting=csv.QUOTE_ALL)
                                    status_changed = update_quote_status_if_fully_submitted(selected_quote_id)
                                    st.success("Supplier quote added successfully.")
                                    if status_changed:
                                        st.success("All parts submitted. Quote status updated to SUBMITTED.")
                                    st.rerun()

