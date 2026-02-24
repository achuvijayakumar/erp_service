import streamlit as st
import json
import pandas as pd
import csv
import uuid
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

    QUOTES_COLUMNS,
    ASSIGNMENTS_COLUMNS,
    WORKER_QUOTES_COLUMNS,
    FINAL_QUOTES_COLUMNS,
    enforce_schema
)

st.set_page_config(layout="wide")


def to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


def render_export_button(label: str, df: pd.DataFrame, filename: str, mime: str = "text/csv"):
    if df is None or df.empty:
        st.info(f"No data available for {label.lower()}.")
        return
    st.download_button(label, to_csv_bytes(df), filename, mime)


def render_import_replace_csv(label: str, expected_columns: list[str], target_path: Path, success_msg: str, key: str):
    #st.caption(f"Warning: This will replace existing records in `{target_path.name}`.")
    uploaded_csv = st.file_uploader(label, type=["csv"], key=key)
    if uploaded_csv is None:
        return

    try:
        df = pd.read_csv(uploaded_csv)
    except Exception as e:
        st.error(f"Invalid CSV: {e}")
        return

    for col in expected_columns:
        if col not in df.columns:
            df[col] = None
    df = df[expected_columns]

    df.to_csv(target_path, index=False, quoting=csv.QUOTE_ALL)
    st.success(success_msg)
    st.rerun()

if "is_authenticated" not in st.session_state:
    st.session_state.is_authenticated = False
    st.session_state.user_id = None
    st.session_state.username = None
    st.session_state.role = None

users_df = read_csv(USERS_PATH)

if not st.session_state.is_authenticated:
    st.title("ERP Login")
    usernames = users_df["USERNAME"].tolist()
    selected_username = st.selectbox("Select User", usernames)

    if st.button("Login"):
        user_row = users_df[
            users_df["USERNAME"] == selected_username
        ].iloc[0]

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
        st.header("Upload Quotes")

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

                if "QUOTE_ID" not in parsed_df.columns:
                    parsed_df.insert(
                        0,
                        "QUOTE_ID",
                        [str(uuid.uuid4().hex[:8]) for _ in range(len(parsed_df))]
                    )

            if not parsed_df.empty:
                st.success(f"{len(parsed_df)} rows parsed successfully")

        if uploaded_file and not parsed_df.empty:
            export_csv = parsed_df.to_csv(index=False).encode("utf-8")

            st.download_button(
                "Download Parsed Data",
                export_csv,
                "parsed_quotes.csv",
                "text/csv"
            )

        quote_table = st.data_editor(
            parsed_df if uploaded_file else pd.DataFrame(columns=[
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
            ]),
            num_rows="dynamic",
            key="quote_editor"
        )

        if not quote_table.empty:
            current_export_csv = quote_table.to_csv(index=False).encode("utf-8")
            st.download_button(
                "Export Current Table",
                current_export_csv,
                "current_quotes.csv",
                "text/csv"
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
                    quote_table["Customer ref NO"] = (
                        quote_table["Customer ref NO"]
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

                        if "QUOTE_ID" not in quote_table.columns:
                            quote_table["QUOTE_ID"] = None
                        quote_table["QUOTE_ID"] = quote_table["QUOTE_ID"].astype("string").str.strip()
                        missing_quote_ids = quote_table["QUOTE_ID"].isna() | (quote_table["QUOTE_ID"] == "")
                        quote_table.loc[missing_quote_ids, "QUOTE_ID"] = [
                            str(uuid.uuid4().hex[:8]) for _ in range(int(missing_quote_ids.sum()))
                        ]

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
        st.header("Master Quotes")

        st.subheader("Data Transfer")
        render_import_replace_csv(
            "Import Quotes CSV",
            QUOTES_COLUMNS,
            QUOTES_PATH,
            "Quotes CSV replaced successfully.",
            key="import_quotes_master"
        )

        quotes_df = read_csv(QUOTES_PATH)

        if not quotes_df.empty:
            st.dataframe(quotes_df)
            render_export_button("Export Quotes CSV", quotes_df, "quotes.csv")
        else:
            st.info("No quotes uploaded yet.")

    # -----------------------
    # TAB 3 - Assign Quotes
    # -----------------------
    with tabs[2]:
        st.header("Assign Quotes to Workers")
        st.subheader("Data Transfer")
        render_import_replace_csv(
            "Import Assignments CSV",
            ASSIGNMENTS_COLUMNS,
            ASSIGNMENTS_PATH,
            "Assignments CSV replaced successfully.",
            key="import_assignments_assign_tab"
        )
        assignments_df = read_csv(ASSIGNMENTS_PATH)
        render_export_button("Export Assignments CSV", assignments_df, "assignments.csv")

        quotes_df = read_csv(QUOTES_PATH)

        if quotes_df.empty:
            st.info("No quotes available.")
        else:
            pending_quotes = quotes_df[
                quotes_df["STATUS"].astype(str).str.upper() == "UPLOADED"
            ]

            if pending_quotes.empty:
                st.info("No unassigned quotes.")
            else:
                worker_df = users_df[users_df["ROLE"] == "worker"].copy()
                worker_list = worker_df["USERNAME"].tolist()

                if not worker_list:
                    st.warning("No workers available for assignment.")
                else:
                    h1, h2, h3, h4, h5, h6, h7 = st.columns([1.2, 2, 3, 1, 1.2, 2, 1])
                    with h1:
                        st.markdown("**QUOTE_ID**")
                    with h2:
                        st.markdown("**PART NO**")
                    with h3:
                        st.markdown("**DESCRIPTION**")
                    with h4:
                        st.markdown("**QTY**")
                    with h5:
                        st.markdown("**STATUS**")
                    with h6:
                        st.markdown("**Assign To**")
                    with h7:
                        st.markdown("**Action**")

                    for idx, row in pending_quotes.iterrows():
                        col1, col2, col3, col4, col5, col6, col7 = st.columns([1.2, 2, 3, 1, 1.2, 2, 1])

                        with col1:
                            st.write(row["QUOTE_ID"])
                        with col2:
                            st.write(row["PART NO"])
                        with col3:
                            st.write(row["DESCRIPTION"])
                        with col4:
                            st.write(row["QTY"])
                        with col5:
                            st.write(row["STATUS"])
                        with col6:
                            selected_worker = st.selectbox(
                                "Assign To",
                                worker_list,
                                key=f"worker_{idx}",
                                label_visibility="collapsed"
                            )
                        with col7:
                            if st.button("Assign", key=f"assign_{idx}"):
                                existing_assignments = read_csv(ASSIGNMENTS_PATH)
                                if existing_assignments.empty:
                                    duplicate = pd.DataFrame()
                                else:
                                    duplicate = existing_assignments[
                                        (existing_assignments["QUOTE_ID"].astype(str) == str(row["QUOTE_ID"])) &
                                        (existing_assignments["PART NO"].astype(str) == str(row["PART NO"]))
                                    ]

                                if not duplicate.empty:
                                    st.warning("Already assigned.")
                                else:
                                    worker_id = worker_df[
                                        worker_df["USERNAME"] == selected_worker
                                    ]["USER_ID"].values[0]

                                    assignment_data = {
                                        "QUOTE_ID": str(row["QUOTE_ID"]),
                                        "PART NO": row["PART NO"],
                                        "ASSIGNED_TO": str(worker_id),
                                        "ASSIGNED_DATE": datetime.now().strftime("%Y-%m-%d")
                                    }

                                    append_to_csv(
                                        ASSIGNMENTS_PATH,
                                        assignment_data,
                                        ASSIGNMENTS_COLUMNS
                                    )

                                    quotes_df.loc[
                                        (quotes_df["QUOTE_ID"].astype(str) == str(row["QUOTE_ID"])) &
                                        (quotes_df["PART NO"].astype(str) == str(row["PART NO"])),
                                        "STATUS"
                                    ] = "ASSIGNED"

                                    quotes_df.to_csv(
                                        QUOTES_PATH,
                                        index=False,
                                        quoting=csv.QUOTE_ALL
                                    )

                                    st.success("Assigned successfully")
                                    st.rerun()

            st.divider()
            st.subheader("Already Assigned")

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
                    assignments_view["PART NO"] = assignments_view["PART NO"].astype(str)
                    assignments_view["ASSIGNED_TO"] = assignments_view["ASSIGNED_TO"].astype(str)

                    users_lookup = users_df[["USER_ID", "USERNAME"]].copy()
                    users_lookup["USER_ID"] = users_lookup["USER_ID"].astype(str)

                    assigned_rows["QUOTE_ID"] = assigned_rows["QUOTE_ID"].astype(str)
                    assigned_rows["PART NO"] = assigned_rows["PART NO"].astype(str)

                    assigned_display = assigned_rows.merge(
                        assignments_view[["QUOTE_ID", "PART NO", "ASSIGNED_TO", "ASSIGNED_DATE"]],
                        on=["QUOTE_ID", "PART NO"],
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
                        "ASSIGNED_DATE"
                    ]]

                    st.dataframe(assigned_display, use_container_width=True)

    # -----------------------
    # TAB 4 - Part Details
    # -----------------------
    with tabs[3]:
        st.header("Part Details")
        st.subheader("Data Transfer")
        render_import_replace_csv(
            "Import Part Details CSV",
            WORKER_QUOTES_COLUMNS,
            WORKER_QUOTES_PATH,
            "Part details CSV replaced successfully.",
            key="import_part_details_admin"
        )

        submissions_df = read_csv(WORKER_QUOTES_PATH)
        if not submissions_df.empty:
            st.dataframe(submissions_df, use_container_width=True)
            render_export_button("Export Part Details CSV", submissions_df, "part_details.csv")
        else:
            st.info("No part details yet.")

    # -----------------------
    # TAB 5 - Margin & Internal Quote
    # -----------------------
    with tabs[4]:
        st.header("Margin & Internal Quote")
        st.subheader("Data Transfer")
        render_import_replace_csv(
            "Import Final Quotes CSV",
            FINAL_QUOTES_COLUMNS,
            FINAL_QUOTES_PATH,
            "Final quotes CSV replaced successfully.",
            key="import_final_quotes_margin"
        )

        worker_df = read_csv(WORKER_QUOTES_PATH)
        quotes_df = read_csv(QUOTES_PATH)

        if quotes_df.empty:
            st.info("No quotes available.")
        elif worker_df.empty:
            st.info("No part details to process.")
        else:
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
                    supplier_base = supplier_base.sort_values(by=["QUOTE_ID", "PART NO", "PRICE"], na_position="last")
                    supplier_base = supplier_base.drop_duplicates(subset=["QUOTE_ID", "PART NO"], keep="first")
                    supplier_base = supplier_base[["QUOTE_ID", "PART NO", "SUPPLIER", "PRICE"]]

                    margin_grid = part_base.merge(
                        supplier_base,
                        on=["QUOTE_ID", "PART NO"],
                        how="left"
                    )

                    if not final_quotes_df.empty:
                        final_existing = final_quotes_df[[
                            "QUOTE_ID",
                            "PART NO",
                            "MARGIN_PERCENT"
                        ]].copy()
                        final_existing["QUOTE_ID"] = final_existing["QUOTE_ID"].astype(str).str.strip()
                        final_existing["PART NO"] = final_existing["PART NO"].astype(str).str.strip()
                        margin_grid = margin_grid.merge(
                            final_existing,
                            on=["QUOTE_ID", "PART NO"],
                            how="left"
                        )
                    else:
                        margin_grid["MARGIN_PERCENT"] = None

                    margin_grid["PRICE"] = pd.to_numeric(margin_grid["PRICE"], errors="coerce").fillna(0.0)
                    margin_grid["QTY"] = pd.to_numeric(margin_grid["QTY"], errors="coerce").fillna(1.0)
                    margin_grid["MARGIN_PERCENT"] = pd.to_numeric(margin_grid["MARGIN_PERCENT"], errors="coerce").fillna(15.0)
                    margin_grid["FINAL_UNIT_PRICE"] = margin_grid["PRICE"] * (1 + margin_grid["MARGIN_PERCENT"] / 100)
                    margin_grid["FINAL_TOTAL"] = margin_grid["FINAL_UNIT_PRICE"] * margin_grid["QTY"]

                    edit_cols = [
                        "QUOTE_ID",
                        "Customer ref NO",
                        "PART NO",
                        "DESCRIPTION",
                        "QTY",
                        "SUPPLIER",
                        "PRICE",
                        "MARGIN_PERCENT",
                        "FINAL_UNIT_PRICE",
                        "FINAL_TOTAL",
                        "DUE DATE"
                    ]
                    st.caption("Set margin for each part in the table below.")
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
                            "PRICE",
                            "FINAL_UNIT_PRICE",
                            "FINAL_TOTAL",
                            "DUE DATE"
                        ]
                    )

                    edited_margin_grid["PRICE"] = pd.to_numeric(edited_margin_grid["PRICE"], errors="coerce").fillna(0.0)
                    edited_margin_grid["QTY"] = pd.to_numeric(edited_margin_grid["QTY"], errors="coerce").fillna(1.0)
                    edited_margin_grid["MARGIN_PERCENT"] = pd.to_numeric(
                        edited_margin_grid["MARGIN_PERCENT"],
                        errors="coerce"
                    ).fillna(0.0)
                    edited_margin_grid["FINAL_UNIT_PRICE"] = edited_margin_grid["PRICE"] * (
                        1 + edited_margin_grid["MARGIN_PERCENT"] / 100
                    )
                    edited_margin_grid["FINAL_TOTAL"] = (
                        edited_margin_grid["FINAL_UNIT_PRICE"] * edited_margin_grid["QTY"]
                    )

                    st.dataframe(
                        edited_margin_grid[
                            [
                                "QUOTE_ID",
                                "PART NO",
                                "SUPPLIER",
                                "PRICE",
                                "MARGIN_PERCENT",
                                "FINAL_UNIT_PRICE",
                                "FINAL_TOTAL"
                            ]
                        ],
                        use_container_width=True
                    )

                    if st.button("Save/Update Final Quotes", key="save_margin_grid"):
                        final_quotes_df = read_csv(FINAL_QUOTES_PATH)
                        if final_quotes_df.empty:
                            final_quotes_df = pd.DataFrame(columns=FINAL_QUOTES_COLUMNS)
                        else:
                            final_quotes_df["QUOTE_ID"] = final_quotes_df["QUOTE_ID"].astype(str).str.strip()
                            final_quotes_df["PART NO"] = final_quotes_df["PART NO"].astype(str).str.strip()

                        for _, row in edited_margin_grid.iterrows():
                            record = {
                                "QUOTE_ID": str(row["QUOTE_ID"]).strip(),
                                "PART NO": str(row["PART NO"]).strip(),
                                "PRICE": float(row["PRICE"]),
                                "MARGIN_PERCENT": float(row["MARGIN_PERCENT"]),
                                "FINAL_UNIT_PRICE": float(row["FINAL_UNIT_PRICE"]),
                                "FINAL_TOTAL": float(row["FINAL_TOTAL"]),
                                "GENERATED_DATE": datetime.now().strftime("%Y-%m-%d")
                            }
                            mask = (
                                (final_quotes_df["QUOTE_ID"].astype(str).str.strip() == record["QUOTE_ID"]) &
                                (final_quotes_df["PART NO"].astype(str).str.strip() == record["PART NO"])
                            )
                            if mask.any():
                                for col, val in record.items():
                                    final_quotes_df.loc[mask, col] = val
                            else:
                                final_quotes_df = pd.concat(
                                    [final_quotes_df, pd.DataFrame([record])],
                                    ignore_index=True
                                )

                        for col in FINAL_QUOTES_COLUMNS:
                            if col not in final_quotes_df.columns:
                                final_quotes_df[col] = None
                        final_quotes_df = final_quotes_df[FINAL_QUOTES_COLUMNS]
                        final_quotes_df.to_csv(FINAL_QUOTES_PATH, index=False, quoting=csv.QUOTE_ALL)
                        st.success("Final quotes saved/updated successfully.")
                        st.rerun()

                st.divider()
                st.subheader("Internal Quote Summary")

                final_quotes_df = read_csv(FINAL_QUOTES_PATH)
                if final_quotes_df.empty:
                    st.info("No final quotes generated yet.")
                else:
                    final_quotes_df["QUOTE_ID"] = final_quotes_df["QUOTE_ID"].astype(str).str.strip()
                    final_quotes_df["PART NO"] = final_quotes_df["PART NO"].astype(str).str.strip()

                    merged_df = final_quotes_df.merge(
                        worker_df,
                        on=["QUOTE_ID", "PART NO"],
                        how="left",
                        suffixes=("_final", "_worker")
                    ).merge(
                        quotes_df,
                        on=["QUOTE_ID", "PART NO"],
                        how="left"
                    )

                    merged_df = merged_df[
                        merged_df["Customer ref NO"].astype(str).str.strip() == str(sel_ref).strip()
                    ]

                    if "PRICE_final" in merged_df.columns:
                        price_final = pd.to_numeric(merged_df["PRICE_final"], errors="coerce")
                    elif "PRICE" in merged_df.columns:
                        price_final = pd.to_numeric(merged_df["PRICE"], errors="coerce")
                    else:
                        price_final = pd.Series(index=merged_df.index, dtype=float)

                    if "PRICE_worker" in merged_df.columns:
                        price_worker = pd.to_numeric(merged_df["PRICE_worker"], errors="coerce")
                    elif "COST_PRICE_EA" in merged_df.columns:
                        price_worker = pd.to_numeric(merged_df["COST_PRICE_EA"], errors="coerce")
                    elif "COST" in merged_df.columns:
                        price_worker = pd.to_numeric(merged_df["COST"], errors="coerce")
                    else:
                        price_worker = pd.Series(index=merged_df.index, dtype=float)

                    merged_df["PRICE"] = price_final.fillna(price_worker)

                    display_columns = [
                        "Customer ref NO",
                        "PART NO",
                        "DESCRIPTION",
                        "QTY",
                        "SUPPLIER",
                        "PRICE",
                        "MARGIN_PERCENT",
                        "FINAL_UNIT_PRICE",
                        "FINAL_TOTAL",
                        "DUE DATE"
                    ]

                    if merged_df.empty:
                        st.info("No final quotes saved yet for this customer reference.")
                    else:
                        summary_df = merged_df[display_columns].rename(
                            columns={"Customer ref NO": "REF NO"}
                        )
                        st.dataframe(summary_df, use_container_width=True)
                        safe_ref = str(sel_ref).replace(" ", "_").replace("/", "-")
                        export_data = summary_df.to_csv(index=False).encode("utf-8")
                        st.download_button(
                            "Download Internal Quote",
                            export_data,
                            f"internal_quote_{safe_ref}.csv",
                            "text/csv"
                        )

    # -----------------------
    # TAB 6 - Export Client Quote
    # -----------------------
    with tabs[5]:
        st.header("Export Client Quote")
        st.subheader("Data Transfer")
        render_import_replace_csv(
            "Import Final Quotes CSV",
            FINAL_QUOTES_COLUMNS,
            FINAL_QUOTES_PATH,
            "Final quotes CSV replaced successfully.",
            key="import_final_quotes_client_export"
        )

        final_df = read_csv(FINAL_QUOTES_PATH)
        worker_df = read_csv(WORKER_QUOTES_PATH)
        quotes_df = read_csv(QUOTES_PATH)

        if final_df.empty:
            st.info("No final quotes generated yet.")
        else:
            # Normalize keys for reliable joins.
            for df in [final_df, worker_df, quotes_df]:
                df["QUOTE_ID"] = df["QUOTE_ID"].astype(str).str.strip()
                df["PART NO"] = df["PART NO"].astype(str).str.strip()

            merged_df = final_df.merge(
                worker_df,
                on=["QUOTE_ID", "PART NO"],
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
                        "CERTIFICATE"
                    ]].rename(columns={
                        "Customer ref NO": "REF NO",
                        "FINAL_UNIT_PRICE": "UNIT PRICE",
                        "FINAL_TOTAL": "TOTAL PRICE",
                        "COND_AVAILABLE": "CONDITION",
                        "LT": "LEAD TIME"
                    })

                    st.dataframe(client_df, use_container_width=True)
                    grand_total = pd.to_numeric(client_df["TOTAL PRICE"], errors="coerce").fillna(0).sum()
                    st.metric("Grand Total", f"{grand_total:,.2f}")

                    csv_data = client_df.to_csv(index=False).encode("utf-8")
                    safe_ref = str(sel_ref).replace(" ", "_").replace("/", "-")
                    st.download_button(
                        "Download Client Quote",
                        csv_data,
                        f"client_quote_{safe_ref}.csv",
                        "text/csv"
                    )


else:
    # -----------------------
    # WORKER VIEW
    # -----------------------
    with tabs[0]:
        st.header("My Assigned Parts")
        assignments_df = read_csv(ASSIGNMENTS_PATH)
        quotes_df = read_csv(QUOTES_PATH)

        # Normalize join keys to avoid dtype/whitespace mismatches.
        assignments_df["QUOTE_ID"] = assignments_df["QUOTE_ID"].astype(str).str.strip()
        assignments_df["PART NO"] = assignments_df["PART NO"].astype(str).str.strip()
        assignments_df["ASSIGNED_TO"] = assignments_df["ASSIGNED_TO"].astype(str).str.strip()
        quotes_df["QUOTE_ID"] = quotes_df["QUOTE_ID"].astype(str).str.strip()
        quotes_df["PART NO"] = quotes_df["PART NO"].astype(str).str.strip()

        my_assignments = assignments_df[
            assignments_df["ASSIGNED_TO"].astype(str) == str(user_id)
        ]

        my_tasks = my_assignments.merge(
            quotes_df,
            on=["QUOTE_ID", "PART NO"],
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
                "DUE DATE"
            ]
            assigned_parts_view = my_tasks[display_columns]
            st.dataframe(assigned_parts_view, use_container_width=True)
            render_export_button(
                "Export Assigned Parts CSV",
                assigned_parts_view,
                f"assigned_parts_{username}.csv"
            )

    with tabs[1]:
        st.header("Submit Supplier Info")
        if not my_tasks.empty:
            pending_tasks_export = my_tasks[["QUOTE_ID", "PART NO", "Customer ref NO", "DESCRIPTION", "QTY", "DUE DATE"]].copy()
            render_export_button(
                "Export My Pending Assigned Tasks CSV",
                pending_tasks_export,
                f"my_pending_tasks_{username}.csv"
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
                        ["NE", "NS", "OH", "SV", "AR"]
                    )
                    qty_available = st.number_input("QTY AVAILABLE", min_value=0, step=1)
                with col2:
                    supplier = st.text_input("SUPPLIER")
                    lt = st.text_input("LT (Lead Time)")
                    certificate = st.text_input("CERTIFICATE")
                    remarks = st.text_area("REMARKS")
                
                submitted = st.form_submit_button("Submit Supplier Quote")
                
                if submitted:
                    if not supplier or cost_price <= 0:
                        st.error("Supplier and Cost Price are required.")
                    else:
                        new_submission = {
                            "QUOTE_ID": sel_quote_id,
                            "PART NO": sel_part_no,
                            "SUPPLIER": supplier,
                            "PRICE": cost_price,
                            "COND_AVAILABLE": cond_available,
                            "QTY_AVAILABLE": qty_available,
                            "LT": lt,
                            "CERTIFICATE": certificate,
                            "REMARKS": remarks,
                            "WORKER_ID": user_id,
                            "SUBMITTED_DATE": datetime.now().strftime("%Y-%m-%d")
                        }
                        
                        append_to_csv(WORKER_QUOTES_PATH, new_submission, WORKER_QUOTES_COLUMNS)
                        st.success("Supplier quotation submitted successfully.")
                        st.rerun()
        else:
            st.info("No tasks assigned to submit info for.")

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

                st.dataframe(my_submissions_view, use_container_width=True)
                render_export_button(
                    "Export My Submissions CSV",
                    my_submissions_view,
                    f"my_submissions_{username}.csv"
                )
