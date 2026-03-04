import streamlit as st
import json
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
    if any(s in {"DELETED", "FINALIZED", "SENT_TO_CLIENT"} for s in current_statuses):
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
         "Assigned Parts",
        "Submit Supplier Info",
        "My Submissions"
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
        "COND",
        "QTY",
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
            "COND",
            "QTY",
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
            "COND",
            "QTY",
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
                    "COND",
                    "QTY",
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
            "COND",
            "QTY",
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
                ~quotes_df["STATUS_NORM"].isin(["SENT_TO_CLIENT", "DELETED"])
            ].copy()
            archive_quotes_df = quotes_df[quotes_df["STATUS_NORM"] == "SENT_TO_CLIENT"].copy()
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
                    st.dataframe(filtered_df)
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
                    st.dataframe(filtered_archive)
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
                    st.dataframe(filtered_deleted)
                    render_export_button(
                        "Export Deleted Quotes Excel",
                        deleted_quotes_df.drop(columns=["STATUS_NORM"], errors="ignore"),
                        "deleted_quotes.xlsx"
                    )
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
            users_lookup = users_df[["USER_ID", "USERNAME"]].copy()
            users_lookup["USER_ID"] = users_lookup["USER_ID"].astype(str).str.strip()
            quotes_lookup = read_csv(QUOTES_PATH)
            if not quotes_lookup.empty:
                quotes_lookup["QUOTE_ID"] = quotes_lookup["QUOTE_ID"].astype(str).str.strip()
                quotes_lookup["PART NO"] = quotes_lookup["PART NO"].astype(str).str.strip()
                quotes_lookup = quotes_lookup[["QUOTE_ID", "PART NO", "Customer ref NO"]].drop_duplicates()

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
            part_details_view = part_details_view.drop(columns=["USER_ID", "WORKER_ID"], errors="ignore")

            filtered_part_details = apply_part_details_filters(
                part_details_view,
                "part_details"
            )
            st.dataframe(filtered_part_details, use_container_width=True)

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
                    st.error("Quote is DELETED and cannot be finalized.")
                    st.stop()
                if current_status == "SENT_TO_CLIENT":
                    st.error("Quote already sent to client. Editing locked.")
                    st.stop()

                selected_quote_id = customer_quotes["QUOTE_ID"].astype(str).str.strip().iloc[0]
                total_parts = customer_quotes[
                    customer_quotes["QUOTE_ID"].astype(str).str.strip() == selected_quote_id
                ]["PART NO"].astype(str).str.strip().nunique()
                submitted_parts = worker_df[
                    worker_df["QUOTE_ID"].astype(str).str.strip() == selected_quote_id
                ]["PART NO"].astype(str).str.strip().nunique()

                if submitted_parts < total_parts:
                    st.warning(
                        f"Only {submitted_parts} out of {total_parts} parts have supplier submissions. "
                        "Showing available submitted parts below."
                    )

                subs_for_customer = worker_df.merge(
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
                    supplier_base = supplier_base[["QUOTE_ID", "PART NO", "SUPPLIER", "PRICE", "CONDITION", "LEAD TIME"]]

                    margin_grid = part_base.merge(
                        supplier_base,
                        on=["QUOTE_ID", "PART NO"],
                        how="left"
                    )

                    if not final_quotes_df.empty:
                        if "SUPPLIER" not in final_quotes_df.columns:
                            final_quotes_df["SUPPLIER"] = ""
                        final_existing = final_quotes_df[[
                            "QUOTE_ID",
                            "PART NO",
                            "SUPPLIER",
                            "MARGIN_PERCENT"
                        ]].copy()
                        final_existing["QUOTE_ID"] = final_existing["QUOTE_ID"].astype(str).str.strip()
                        final_existing["PART NO"] = final_existing["PART NO"].astype(str).str.strip()
                        final_existing["SUPPLIER"] = final_existing["SUPPLIER"].astype(str).str.strip()
                        margin_grid = margin_grid.merge(
                            final_existing,
                            on=["QUOTE_ID", "PART NO", "SUPPLIER"],
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

                    st.subheader("Internal Quote Summary")
                    edit_cols = [
                        "SELECT",
                        "QUOTE_ID",
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
                        "DUE DATE"
                    ]
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
                            "CONDITION",
                            "LEAD TIME",
                            "PRICE",
                            "TOTAL PRICE",
                            "FINAL_UNIT_PRICE",
                            "FINAL_TOTAL",
                            "DUE DATE"
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
                        st.info("No suppliers selected. Nothing will be saved or finalized.")
                    else:
                        st.dataframe(
                            selected_preview[preview_columns],
                            use_container_width=True
                        )

                    col_save, col_finalize = st.columns(2)

                    with col_save:
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

                            for _, row in selected_rows.iterrows():
                                record = {
                                    "QUOTE_ID": str(row["QUOTE_ID"]).strip(),
                                    "PART NO": str(row["PART NO"]).strip(),
                                    "SUPPLIER": str(row["SUPPLIER"]).strip(),
                                    "PRICE": float(row["PRICE"]),
                                    "MARGIN_PERCENT": float(row["MARGIN_PERCENT"]),
                                    "FINAL_UNIT_PRICE": float(row["FINAL_UNIT_PRICE"]),
                                    "FINAL_TOTAL": float(row["FINAL_TOTAL"]),
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

                    with col_finalize:
                        if st.button("Finalize Quote", key="finalize_margin_quote"):
                            final_df_check = read_csv(FINAL_QUOTES_PATH)
                            if final_df_check.empty:
                                has_rows = False
                            else:
                                final_df_check["QUOTE_ID"] = final_df_check["QUOTE_ID"].astype(str).str.strip()
                                has_rows = not final_df_check[
                                    final_df_check["QUOTE_ID"] == str(selected_quote_id).strip()
                                ].empty

                            if not has_rows:
                                st.error("Cannot finalize. No selected supplier rows saved.")
                                st.stop()

                            edited_margin_grid["SELECT"] = (
                                edited_margin_grid["SELECT"]
                                .fillna(False)
                                .apply(lambda x: x if isinstance(x, bool) else str(x).strip().lower() == "true")
                            )
                            valid_supplier_mask = ~edited_margin_grid["SUPPLIER"].astype(str).str.strip().str.lower().isin(["", "nan", "none"])
                            current_selected = edited_margin_grid[
                                (edited_margin_grid["SELECT"] == True) & valid_supplier_mask
                            ].copy()

                            compare_cols = [
                                "QUOTE_ID",
                                "PART NO",
                                "SUPPLIER",
                                "PRICE",
                                "MARGIN_PERCENT",
                                "FINAL_UNIT_PRICE",
                                "FINAL_TOTAL",
                            ]
                            key_cols = ["QUOTE_ID", "PART NO", "SUPPLIER"]
                            num_cols = ["PRICE", "MARGIN_PERCENT", "FINAL_UNIT_PRICE", "FINAL_TOTAL"]

                            current_compare = current_selected[compare_cols].copy() if not current_selected.empty else pd.DataFrame(columns=compare_cols)
                            for col in key_cols:
                                current_compare[col] = current_compare[col].astype(str).str.strip()
                            for col in num_cols:
                                current_compare[col] = pd.to_numeric(current_compare[col], errors="coerce").fillna(0.0).round(6)
                            current_compare = current_compare.sort_values(by=compare_cols).reset_index(drop=True)

                            saved_rows = final_df_check[
                                final_df_check["QUOTE_ID"].astype(str).str.strip() == str(selected_quote_id).strip()
                            ].copy()
                            if "SUPPLIER" not in saved_rows.columns:
                                saved_rows["SUPPLIER"] = ""
                            for col in compare_cols:
                                if col not in saved_rows.columns:
                                    saved_rows[col] = None
                            saved_compare = saved_rows[compare_cols].copy()
                            for col in key_cols:
                                saved_compare[col] = saved_compare[col].astype(str).str.strip()
                            for col in num_cols:
                                saved_compare[col] = pd.to_numeric(saved_compare[col], errors="coerce").fillna(0.0).round(6)
                            saved_compare = saved_compare.sort_values(by=compare_cols).reset_index(drop=True)

                            if not current_compare.equals(saved_compare):
                                st.error("Unsaved changes detected. Please Save Draft first.")
                                st.stop()

                            quotes_df = read_csv(QUOTES_PATH)

                            ref_mask = (
                                quotes_df["Customer ref NO"]
                                .astype(str)
                                .str.strip()
                                == str(sel_ref).strip()
                            )

                            quotes_df.loc[ref_mask, "STATUS"] = "FINALIZED"

                            quotes_df.to_csv(
                                QUOTES_PATH,
                                index=False,
                                quoting=csv.QUOTE_ALL
                            )

                            st.success("Quote finalized successfully.")
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
                            "DUE DATE"
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
                            if "SUPPLIER" not in saved_final_df.columns:
                                saved_final_df["SUPPLIER"] = ""
                            saved_final_df["SUPPLIER"] = saved_final_df["SUPPLIER"].astype(str).str.strip()

                            selected_quote_ids = customer_quotes["QUOTE_ID"].astype(str).str.strip().unique().tolist()
                            saved_final_df = saved_final_df[
                                saved_final_df["QUOTE_ID"].isin(selected_quote_ids)
                            ]

                            cert_df = read_csv(WORKER_QUOTES_PATH)
                            if not cert_df.empty and "CERTIFICATE_FILE" in cert_df.columns:
                                cert_df["QUOTE_ID"] = cert_df["QUOTE_ID"].astype(str).str.strip()
                                cert_df["PART NO"] = cert_df["PART NO"].astype(str).str.strip()
                                cert_df["SUPPLIER"] = cert_df["SUPPLIER"].astype(str).str.strip()
                                cert_df["CERTIFICATE_FILE"] = cert_df["CERTIFICATE_FILE"].fillna("").astype(str).str.strip()

                                added_files = set()
                                for _, frow in saved_final_df.iterrows():
                                    match = cert_df[
                                        (cert_df["QUOTE_ID"] == str(frow["QUOTE_ID"]).strip()) &
                                        (cert_df["PART NO"] == str(frow["PART NO"]).strip()) &
                                        (cert_df["SUPPLIER"] == str(frow["SUPPLIER"]).strip())
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
            if "SUPPLIER" not in final_df.columns:
                final_df["SUPPLIER"] = ""
            final_df["SUPPLIER"] = final_df["SUPPLIER"].astype(str).str.strip()
            worker_df["SUPPLIER"] = worker_df["SUPPLIER"].astype(str).str.strip()

            merged_df = final_df.merge(
                worker_df,
                on=["QUOTE_ID", "PART NO", "SUPPLIER"],
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
                    client_df = merged_df[[
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
                        "Customer ref NO": "REF NO",
                        "FINAL_UNIT_PRICE": "UNIT PRICE",
                        "FINAL_TOTAL": "TOTAL PRICE",
                        "COND_AVAILABLE": "CONDITION",
                        "LT": "LEAD TIME (DAYS)",
                        "CERTIFICATE_TYPE": "CERTIFICATE TYPE"
                    })

                    st.dataframe(client_df, width='stretch')
                    excel_data = to_excel_bytes(client_df)
                    safe_ref = str(sel_ref).replace(" ", "_").replace("/", "-")
                    zip_buffer = io.BytesIO()
                    with zipfile.ZipFile(zip_buffer, "w") as zf:
                        zf.writestr("client_quote.xlsx", excel_data)

                        selected_quote_ids = merged_df["QUOTE_ID"].astype(str).str.strip().unique().tolist()
                        final_rows_for_ref = final_df[
                            final_df["QUOTE_ID"].astype(str).str.strip().isin(selected_quote_ids)
                        ].copy()
                        if not final_rows_for_ref.empty and "CERTIFICATE_FILE" in worker_df.columns:
                            worker_df["CERTIFICATE_FILE"] = worker_df["CERTIFICATE_FILE"].fillna("").astype(str).str.strip()
                            added_files = set()
                            for _, frow in final_rows_for_ref.iterrows():
                                match = worker_df[
                                    (worker_df["QUOTE_ID"] == str(frow["QUOTE_ID"]).strip()) &
                                    (worker_df["PART NO"] == str(frow["PART NO"]).strip()) &
                                    (worker_df["SUPPLIER"] == str(frow["SUPPLIER"]).strip())
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
                        "Download Client Quote",
                        zip_buffer.getvalue(),
                        f"client_quote_{safe_ref}.zip",
                        "application/zip"
                    )
                    if st.button("Mark as SENT_TO_CLIENT", key="mark_sent_to_client"):
                        quotes_df = read_csv(QUOTES_PATH)
                        ref_mask = quotes_df["Customer ref NO"].astype(str).str.strip() == str(sel_ref).strip()
                        quotes_df.loc[ref_mask, "STATUS"] = "SENT_TO_CLIENT"
                        quotes_df.to_csv(QUOTES_PATH, index=False, quoting=csv.QUOTE_ALL)
                        st.success("Quote marked as SENT_TO_CLIENT.")
                        st.rerun()


else:
    # ===========================
    # WORKER VIEW
    # ===========================

    # -----------------------
    # WORKER TAB 1 - Assigned Parts
    # -----------------------
    with tabs[0]:
        st.header("My Assigned Parts")
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
        if my_tasks.empty:
            st.info("No parts assigned to you yet.")
        else:
            if "MEASURE_UNIT" not in my_tasks.columns:
                my_tasks["MEASURE_UNIT"] = "EA"

            display_columns = [
                "QUOTE_ID",
                "Customer ref NO",
                "PART NO",
                "DESCRIPTION",
                "QTY",
                "MEASURE_UNIT",
                "WORKER_DUE_DATE"
            ]
            assigned_parts_view = my_tasks[display_columns]
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
        if not my_tasks.empty:
            pending_tasks_export = my_tasks[["QUOTE_ID", "PART NO", "Customer ref NO", "DESCRIPTION", "QTY", "WORKER_DUE_DATE"]].copy()
            render_export_button(
                "Export My Pending Assigned Tasks Excel",
                pending_tasks_export,
                f"my_pending_tasks_{username}.xlsx"
            )
        
        if not my_tasks.empty:
            # We need QUOTE_ID + PART NO to identify the task uniquely
            task_options = my_tasks.apply(lambda x: f"{x['QUOTE_ID']} - {x['PART NO']}", axis=1).tolist()
            selected_task = st.selectbox("Select Assigned Task", task_options)
            
            # Extract ID and Part
            sel_quote_id = selected_task.split(" - ")[0]
            sel_part_no = selected_task.split(" - ")[1]
            
            with st.form("submission_form"):
                col1, col2 = st.columns(2)
                with col1:
                    cost_price = st.number_input("PRICE", min_value=0.0, step=0.01)
                    cond_available = st.selectbox(
                        "COND AVAILABLE",
                        ["NE", "NS", "OH", "SV", "AR","FN", "MOD"]
                    )
                    qty_available = st.number_input("QTY AVAILABLE", min_value=0, step=1)
                with col2:
                    supplier = st.text_input("SUPPLIER")
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
                    if not supplier or cost_price <= 0:
                        st.error("Supplier and Cost Price are required.")
                    elif certificate_available and uploaded_cert is None:
                        st.error("Please upload certificate file.")
                        st.stop()
                    else:
                        new_submission = {
                            "QUOTE_ID": sel_quote_id,
                            "PART NO": sel_part_no,
                            "SUPPLIER": supplier,
                            "PRICE": cost_price,
                            "COND_AVAILABLE": cond_available,
                            "QTY_AVAILABLE": qty_available,
                            "LT": lt,
                            "CERTIFICATE_AVAILABLE": "YES" if certificate_available else "NO",
                            "CERTIFICATE_FILE": None,
                            "CERTIFICATE_TYPE": certificate_type if certificate_available else "",
                            "REMARKS": remarks,
                            "WORKER_ID": user_id,
                            "SUBMITTED_DATE": datetime.now().strftime("%Y-%m-%d")
                        }

                        if certificate_available and uploaded_cert is not None:
                            safe_quote_id = safe_filename_part(sel_quote_id)
                            safe_part_no = safe_filename_part(sel_part_no)
                            safe_supplier = safe_filename_part(supplier)
                            cert_filename = f"{safe_quote_id}_{safe_part_no}_{safe_supplier}.pdf"
                            cert_path = CERTIFICATE_DIR / cert_filename
                            cert_path.write_bytes(uploaded_cert.getbuffer())
                            new_submission["CERTIFICATE_FILE"] = cert_filename
                        
                        append_to_csv(WORKER_QUOTES_PATH, new_submission, WORKER_QUOTES_COLUMNS)
                        status_changed = update_quote_status_if_fully_submitted(sel_quote_id)
                        st.success("Supplier quotation submitted successfully.")
                        if status_changed:
                            st.success("All parts submitted. Quote status updated to SUBMITTED.")
                        st.rerun()
        else:
            st.info("No tasks assigned to submit info for.")

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
            worker_df["QUOTE_ID"] = worker_df["QUOTE_ID"].astype(str).str.strip()
            worker_df["PART NO"] = worker_df["PART NO"].astype(str).str.strip()
            quotes_df["QUOTE_ID"] = quotes_df["QUOTE_ID"].astype(str).str.strip()
            quotes_df["PART NO"] = quotes_df["PART NO"].astype(str).str.strip()

            my_quotes = worker_df[
                worker_df["WORKER_ID"].astype(str).str.strip() == str(user_id)
            ]

            if my_quotes.empty:
                st.info("No submissions yet.")
            else:
                merged = my_quotes.merge(
                    quotes_df,
                    on=["QUOTE_ID", "PART NO"],
                    how="left"
                )
                merged = merged[merged["STATUS"].astype(str).str.strip().str.upper() != "DELETED"]
                if merged.empty:
                    st.info("No submissions yet.")
                    st.stop()

                display_cols = [
                    "Customer ref NO",
                    "PART NO",
                    "DESCRIPTION",
                    "PRICE",
                    "COND_AVAILABLE",
                    "QTY_AVAILABLE",
                    "LT",
                    "SUBMITTED_DATE"
                ]

                my_submissions_view = merged[display_cols].rename(columns={
                    "Customer ref NO": "REF NO",
                    "COND_AVAILABLE": "CONDITION",
                    "LT": "LEAD TIME",
                    "SUBMITTED_DATE": "SUBMITTED DATE"
                })

                filtered_my_submissions_view = apply_my_submissions_filters(
                    my_submissions_view,
                    "worker_my_submissions"
                )
                st.dataframe(filtered_my_submissions_view, width='stretch')
                render_export_button(
                    "Export My Submissions Excel",
                    filtered_my_submissions_view,
                    f"my_submissions_{username}.xlsx"
                )
