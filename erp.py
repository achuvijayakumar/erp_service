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
    ORDERS_PATH,
    ASSIGNMENTS_PATH,
    WORKER_QUOTES_PATH,
    FINAL_QUOTES_PATH,

    ORDERS_COLUMNS,
    ASSIGNMENTS_COLUMNS,
    WORKER_QUOTES_COLUMNS,
    FINAL_QUOTES_COLUMNS,
    enforce_schema
)

st.set_page_config(layout="wide")

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
        "Upload Orders",
        "Master Orders",
        "Assign Orders",
        "Worker Submissions",
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
    # TAB 1 - Upload Orders
    # -------------------------
    with tabs[0]:
        st.header("Upload Orders")

        uploaded_file = st.file_uploader(
            "Upload Excel File",
            type=["xlsx", "xls"]
        )

        parsed_df = pd.DataFrame(columns=[
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

            if not parsed_df.empty:
                st.success(f"{len(parsed_df)} rows parsed successfully")

        if uploaded_file and not parsed_df.empty:
            export_csv = parsed_df.to_csv(index=False).encode("utf-8")

            st.download_button(
                "Download Parsed Data",
                export_csv,
                "parsed_orders.csv",
                "text/csv"
            )

        order_table = st.data_editor(
            parsed_df if uploaded_file else pd.DataFrame(columns=[
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
            key="order_editor"
        )

        if not order_table.empty:
            current_export_csv = order_table.to_csv(index=False).encode("utf-8")
            st.download_button(
                "Export Current Table",
                current_export_csv,
                "current_orders.csv",
                "text/csv"
            )

        if st.button("Save Orders"):

            if order_table.empty:
                st.warning("No data to save")
            else:
                order_table = order_table.copy()

                # Validate QTY
                order_table["QTY"] = pd.to_numeric(
                    order_table["QTY"], errors="coerce"
                )

                order_table["DATE"] = pd.to_datetime(
                    order_table["DATE"],
                    errors="coerce"
                )
                order_table["DUE DATE"] = pd.to_datetime(
                    order_table["DUE DATE"],
                    errors="coerce"
                )
                order_table["COND"] = order_table["COND"].astype("string").str.upper()

                order_table = order_table.dropna(
                    subset=["PART NO", "QTY", "DATE", "DUE DATE"]
                )

                if order_table.empty:
                    st.error("Invalid data")
                else:
                    save_blocked = False
                    order_table["Customer ref NO"] = order_table["Customer ref NO"].astype(str).str.strip()
                    order_table = order_table[order_table["Customer ref NO"] != ""]
                    if order_table.empty:
                        st.error("Customer ref NO is required.")
                        save_blocked = True

                    batch_refs = order_table["Customer ref NO"].dropna().unique().tolist()
                    if not save_blocked and len(batch_refs) > 1:
                        st.error("Only one Customer ref NO is allowed per upload/save batch.")
                        save_blocked = True

                    existing = read_csv(ORDERS_PATH)
                    existing_refs = (
                        existing["Customer ref NO"].astype(str).str.strip().tolist()
                        if not existing.empty and "Customer ref NO" in existing.columns
                        else []
                    )
                    duplicate_refs = [r for r in batch_refs if r in existing_refs]
                    if not save_blocked and duplicate_refs:
                        st.error(
                            f"Customer Ref No already exists: {', '.join(duplicate_refs)}. "
                            "Use a unique Customer ref NO for each RFQ batch."
                        )
                        save_blocked = True

                    if not save_blocked:
                        order_table["DATE"] = order_table["DATE"].dt.strftime("%Y-%m-%d")
                        order_table["DUE DATE"] = order_table["DUE DATE"].dt.strftime("%Y-%m-%d")

                        order_table["ORDER_ID"] = [
                            str(uuid.uuid4().hex[:8])
                            for _ in range(len(order_table))
                        ]

                        order_table["STATUS"] = "UPLOADED"
                        order_table["CREATED_DATE"] = datetime.now().strftime("%Y-%m-%d")

                        # Enforce Schema & Types
                        for col in ORDERS_COLUMNS:
                            if col not in order_table.columns:
                                order_table[col] = None
                        
                        order_table["ORDER_ID"] = order_table["ORDER_ID"].astype(str)

                        order_table = order_table[ORDERS_COLUMNS]

                        # Safe Rewrite (No append mode)
                        final_df = pd.concat([existing, order_table])

                        final_df.to_csv(
                            ORDERS_PATH,
                            index=False,
                            quoting=csv.QUOTE_ALL
                        )

                        st.success(f"Saved {len(order_table)} orders")
    # ------------------------
    # TAB 2 - Master Orders
    # -------------------------
    with tabs[1]:
        st.header("Master Orders")

        orders_df = read_csv(ORDERS_PATH)

        if not orders_df.empty:
            st.dataframe(orders_df)
        else:
            st.info("No orders uploaded yet.")

    # -----------------------
    # TAB 3 - Assign Orders
    # -----------------------
    with tabs[2]:
        st.header("Assign Orders to Workers")
        orders_df = read_csv(ORDERS_PATH)

        if orders_df.empty:
            st.info("No orders available.")
        else:
            pending_orders = orders_df[
                orders_df["STATUS"].astype(str).str.upper() == "UPLOADED"
            ]

            if pending_orders.empty:
                st.info("No unassigned orders.")
            else:
                worker_df = users_df[users_df["ROLE"] == "worker"].copy()
                worker_list = worker_df["USERNAME"].tolist()

                if not worker_list:
                    st.warning("No workers available for assignment.")
                else:
                    h1, h2, h3, h4, h5, h6, h7 = st.columns([1.2, 2, 3, 1, 1.2, 2, 1])
                    with h1:
                        st.markdown("**ORDER_ID**")
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

                    for idx, row in pending_orders.iterrows():
                        col1, col2, col3, col4, col5, col6, col7 = st.columns([1.2, 2, 3, 1, 1.2, 2, 1])

                        with col1:
                            st.write(row["ORDER_ID"])
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
                                        (existing_assignments["ORDER_ID"].astype(str) == str(row["ORDER_ID"])) &
                                        (existing_assignments["PART NO"].astype(str) == str(row["PART NO"]))
                                    ]

                                if not duplicate.empty:
                                    st.warning("Already assigned.")
                                else:
                                    worker_id = worker_df[
                                        worker_df["USERNAME"] == selected_worker
                                    ]["USER_ID"].values[0]

                                    assignment_data = {
                                        "ORDER_ID": str(row["ORDER_ID"]),
                                        "PART NO": row["PART NO"],
                                        "ASSIGNED_TO": str(worker_id),
                                        "ASSIGNED_DATE": datetime.now().strftime("%Y-%m-%d")
                                    }

                                    append_to_csv(
                                        ASSIGNMENTS_PATH,
                                        assignment_data,
                                        ASSIGNMENTS_COLUMNS
                                    )

                                    orders_df.loc[
                                        (orders_df["ORDER_ID"].astype(str) == str(row["ORDER_ID"])) &
                                        (orders_df["PART NO"].astype(str) == str(row["PART NO"])),
                                        "STATUS"
                                    ] = "ASSIGNED"

                                    orders_df.to_csv(
                                        ORDERS_PATH,
                                        index=False,
                                        quoting=csv.QUOTE_ALL
                                    )

                                    st.success("Assigned successfully")
                                    st.rerun()

            st.divider()
            st.subheader("Already Assigned")

            assignments_df = read_csv(ASSIGNMENTS_PATH)
            if assignments_df.empty:
                st.info("No assigned rows yet.")
            else:
                assigned_rows = orders_df[
                    orders_df["STATUS"].astype(str).str.upper() == "ASSIGNED"
                ][["ORDER_ID", "PART NO", "DESCRIPTION", "QTY", "STATUS"]].copy()

                if assigned_rows.empty:
                    st.info("No assigned rows yet.")
                else:
                    assignments_view = assignments_df.copy()
                    assignments_view["ORDER_ID"] = assignments_view["ORDER_ID"].astype(str)
                    assignments_view["PART NO"] = assignments_view["PART NO"].astype(str)
                    assignments_view["ASSIGNED_TO"] = assignments_view["ASSIGNED_TO"].astype(str)

                    users_lookup = users_df[["USER_ID", "USERNAME"]].copy()
                    users_lookup["USER_ID"] = users_lookup["USER_ID"].astype(str)

                    assigned_rows["ORDER_ID"] = assigned_rows["ORDER_ID"].astype(str)
                    assigned_rows["PART NO"] = assigned_rows["PART NO"].astype(str)

                    assigned_display = assigned_rows.merge(
                        assignments_view[["ORDER_ID", "PART NO", "ASSIGNED_TO", "ASSIGNED_DATE"]],
                        on=["ORDER_ID", "PART NO"],
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
                        "ORDER_ID",
                        "PART NO",
                        "DESCRIPTION",
                        "QTY",
                        "STATUS",
                        "ASSIGNED_TO_USER",
                        "ASSIGNED_DATE"
                    ]]

                    st.dataframe(assigned_display, use_container_width=True)

    # -----------------------
    # TAB 4 - Worker Submissions
    # -----------------------
    with tabs[3]:
        st.header("Worker Submissions")
        submissions_df = read_csv(WORKER_QUOTES_PATH)
        if not submissions_df.empty:
            st.dataframe(submissions_df, use_container_width=True)
        else:
            st.info("No submissions from workers yet.")

    # -----------------------
    # TAB 5 - Margin & Internal Quote
    # -----------------------
    with tabs[4]:
        st.header("Margin & Internal Quote")
        worker_df = read_csv(WORKER_QUOTES_PATH)
        orders_df = read_csv(ORDERS_PATH)

        if orders_df.empty:
            st.info("No orders available.")
        elif worker_df.empty:
            st.info("No worker submissions to process.")
        else:
            for df in [worker_df, orders_df]:
                df["ORDER_ID"] = df["ORDER_ID"].astype(str).str.strip()
                df["PART NO"] = df["PART NO"].astype(str).str.strip()

            customer_refs = (
                orders_df["Customer ref NO"]
                .dropna()
                .astype(str)
                .str.strip()
                .unique()
                .tolist()
            )

            if not customer_refs:
                st.info("No customer references found in orders.")
            else:
                sel_ref = st.selectbox("Select Customer Ref", customer_refs, key="margin_customer_ref")
                customer_orders = orders_df[
                    orders_df["Customer ref NO"].astype(str).str.strip() == str(sel_ref).strip()
                ].copy()

                subs_for_customer = worker_df.merge(
                    customer_orders[["ORDER_ID", "PART NO"]].drop_duplicates(),
                    on=["ORDER_ID", "PART NO"],
                    how="inner"
                )

                final_quotes_df = read_csv(FINAL_QUOTES_PATH)

                if subs_for_customer.empty:
                    st.info("No worker submissions found for this customer reference.")
                else:
                    for idx, row in subs_for_customer.iterrows():
                        with st.expander(f"Part: {row['PART NO']} (Supplier: {row['SUPPLIER']})"):
                            # Keep compatibility with older worker quote schemas.
                            if "PRICE" in subs_for_customer.columns:
                                cost_col = "PRICE"
                            elif "COST_PRICE_EA" in subs_for_customer.columns:
                                cost_col = "COST_PRICE_EA"
                            else:
                                cost_col = "COST"
                            cost = pd.to_numeric(row.get(cost_col, 0), errors="coerce")
                            if pd.isna(cost):
                                cost = 0.0
                            st.write(f"Supplier Price: {cost}")

                            qty_row = customer_orders[
                                (customer_orders["ORDER_ID"] == str(row["ORDER_ID"]).strip()) &
                                (customer_orders["PART NO"] == str(row["PART NO"]).strip())
                            ]
                            qty = qty_row["QTY"].values[0] if not qty_row.empty else 1

                            margin_percent = st.number_input(
                                f"Margin %",
                                min_value=0.0,
                                value=15.0,
                                key=f"margin_{idx}"
                            )
                            final_unit = cost * (1 + margin_percent / 100)
                            final_total = final_unit * qty

                            st.write(f"Final Unit Price: {final_unit:.2f}")
                            st.write(f"Final Total: {final_total:.2f}")

                            if st.button("Save Final Quote", key=f"save_{idx}"):
                                existing_final = final_quotes_df[
                                    (final_quotes_df["ORDER_ID"].astype(str).str.strip() == str(row["ORDER_ID"]).strip()) &
                                    (final_quotes_df["PART NO"].astype(str).str.strip() == str(row["PART NO"]).strip())
                                ] if not final_quotes_df.empty else pd.DataFrame()

                                if not existing_final.empty:
                                    st.warning("Final quote already exists for this part.")
                                else:
                                    final_quote = {
                                        "ORDER_ID": str(row["ORDER_ID"]).strip(),
                                        "PART NO": str(row["PART NO"]).strip(),
                                        "PRICE": cost,
                                        "MARGIN_PERCENT": margin_percent,
                                        "FINAL_UNIT_PRICE": final_unit,
                                        "FINAL_TOTAL": final_total,
                                        "GENERATED_DATE": datetime.now().strftime("%Y-%m-%d")
                                    }
                                    append_to_csv(FINAL_QUOTES_PATH, final_quote, FINAL_QUOTES_COLUMNS)
                                    st.success(f"Saved quote for {row['PART NO']}")
                                    st.rerun()

                st.divider()
                st.subheader("Internal Quote Summary")

                final_quotes_df = read_csv(FINAL_QUOTES_PATH)
                if final_quotes_df.empty:
                    st.info("No final quotes generated yet.")
                else:
                    final_quotes_df["ORDER_ID"] = final_quotes_df["ORDER_ID"].astype(str).str.strip()
                    final_quotes_df["PART NO"] = final_quotes_df["PART NO"].astype(str).str.strip()

                    merged_df = final_quotes_df.merge(
                        worker_df,
                        on=["ORDER_ID", "PART NO"],
                        how="left",
                        suffixes=("_final", "_worker")
                    ).merge(
                        orders_df,
                        on=["ORDER_ID", "PART NO"],
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
        final_df = read_csv(FINAL_QUOTES_PATH)
        worker_df = read_csv(WORKER_QUOTES_PATH)
        orders_df = read_csv(ORDERS_PATH)

        if final_df.empty:
            st.info("No final quotes generated yet.")
        else:
            # Normalize keys for reliable joins.
            for df in [final_df, worker_df, orders_df]:
                df["ORDER_ID"] = df["ORDER_ID"].astype(str).str.strip()
                df["PART NO"] = df["PART NO"].astype(str).str.strip()

            merged_df = final_df.merge(
                worker_df,
                on=["ORDER_ID", "PART NO"],
                how="left"
            )
            merged_df = merged_df.merge(
                orders_df,
                on=["ORDER_ID", "PART NO"],
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
        orders_df = read_csv(ORDERS_PATH)

        # Normalize join keys to avoid dtype/whitespace mismatches.
        assignments_df["ORDER_ID"] = assignments_df["ORDER_ID"].astype(str).str.strip()
        assignments_df["PART NO"] = assignments_df["PART NO"].astype(str).str.strip()
        assignments_df["ASSIGNED_TO"] = assignments_df["ASSIGNED_TO"].astype(str).str.strip()
        orders_df["ORDER_ID"] = orders_df["ORDER_ID"].astype(str).str.strip()
        orders_df["PART NO"] = orders_df["PART NO"].astype(str).str.strip()

        my_assignments = assignments_df[
            assignments_df["ASSIGNED_TO"].astype(str) == str(user_id)
        ]

        my_tasks = my_assignments.merge(
            orders_df,
            on=["ORDER_ID", "PART NO"],
            how="left"
        )
        if my_tasks.empty:
            st.info("No parts assigned to you yet.")
        else:
            if "MEASURE_UNIT" not in my_tasks.columns:
                my_tasks["MEASURE_UNIT"] = "EA"

            display_columns = [
                "Customer ref NO",
                "PART NO",
                "DESCRIPTION",
                "QTY",
                "MEASURE_UNIT",
                "DUE DATE"
            ]
            st.dataframe(my_tasks[display_columns], use_container_width=True)

    with tabs[1]:
        st.header("Submit Supplier Info")
        
        if not my_tasks.empty:
            # We need ORDER_ID + PART NO to identify the task uniquely
            task_options = my_tasks.apply(lambda x: f"{x['ORDER_ID']} - {x['PART NO']}", axis=1).tolist()
            selected_task = st.selectbox("Select Assigned Task", task_options)
            
            # Extract ID and Part
            sel_order_id = selected_task.split(" - ")[0]
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
                            "ORDER_ID": sel_order_id,
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
        orders_df = read_csv(ORDERS_PATH)

        if worker_df.empty:
            st.info("No submissions yet.")
        else:
            worker_df["ORDER_ID"] = worker_df["ORDER_ID"].astype(str).str.strip()
            worker_df["PART NO"] = worker_df["PART NO"].astype(str).str.strip()
            orders_df["ORDER_ID"] = orders_df["ORDER_ID"].astype(str).str.strip()
            orders_df["PART NO"] = orders_df["PART NO"].astype(str).str.strip()

            my_quotes = worker_df[
                worker_df["WORKER_ID"].astype(str).str.strip() == str(user_id)
            ]

            if my_quotes.empty:
                st.info("No submissions yet.")
            else:
                merged = my_quotes.merge(
                    orders_df,
                    on=["ORDER_ID", "PART NO"],
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
