import pandas as pd
from common import get_db_connection, execute_query
from sqlalchemy import text


UNKNOWN_CUSTOMER = "Unknown"


def load_dwh():
    print("ETL Step 3: Loading Data Warehouse...")
    engine = get_db_connection()

    # ---------------------------------------------------------
    # 1. Load Dimensions
    # ---------------------------------------------------------

    # --- Dim Country ---
    print("Loading Dim Country...")
    execute_query(
        text(
            """
        INSERT INTO ecom_dwh.dim_country (country_name)
        SELECT DISTINCT country
        FROM ecom_staging.stg_sales
        WHERE country IS NOT NULL
        ON CONFLICT (country_name) DO NOTHING;
        """
        )
    )

    # --- Dim Product ---
    print("Loading Dim Product...")
    execute_query(
        text(
            """
        INSERT INTO ecom_dwh.dim_product (stock_code, description)
        SELECT DISTINCT ON (stock_code) stock_code, description
        FROM ecom_staging.stg_sales
        WHERE stock_code IS NOT NULL
        ORDER BY stock_code, invoice_date DESC
        ON CONFLICT (stock_code)
        DO UPDATE SET description = EXCLUDED.description;
        """
        )
    )

    # --- Dim Customer ---
    print("Loading Dim Customer...")

    # 1) Ensure Unknown customer exists (TEXT id)
    execute_query(
        text(
            """
        INSERT INTO ecom_dwh.dim_customer (customer_id, is_unknown)
        VALUES (:unknown_id, TRUE)
        ON CONFLICT (customer_id) DO NOTHING;
        """
        ).bindparams(unknown_id=UNKNOWN_CUSTOMER)
    )

    # 2) Insert real customers (exclude empty strings too)
    execute_query(
        text(
            """
        INSERT INTO ecom_dwh.dim_customer (customer_id, is_unknown)
        SELECT DISTINCT customer_id, FALSE
        FROM ecom_staging.stg_sales
        WHERE customer_id IS NOT NULL
          AND TRIM(customer_id) <> ''
        ON CONFLICT (customer_id) DO NOTHING;
        """
        )
    )

    # --- Dim Date ---
    print("Loading Dim Date...")
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT MIN(invoice_date), MAX(invoice_date) FROM ecom_staging.stg_sales")
        )
        min_date, max_date = result.fetchone()

    if min_date and max_date:
        start_date = min_date - pd.Timedelta(days=30)
        end_date = max_date + pd.Timedelta(days=30)

        date_df = pd.DataFrame({"date": pd.date_range(start_date, end_date)})
        date_df["date_key"] = date_df["date"].dt.strftime("%Y%m%d").astype(int)
        date_df["full_date"] = date_df["date"].dt.date
        date_df["year"] = date_df["date"].dt.year
        date_df["quarter"] = date_df["date"].dt.quarter
        date_df["month"] = date_df["date"].dt.month
        date_df["month_name"] = date_df["date"].dt.month_name()
        date_df["day"] = date_df["date"].dt.day
        date_df["weekday_name"] = date_df["date"].dt.day_name()

        existing_keys = pd.read_sql(
            "SELECT date_key FROM ecom_dwh.dim_date", engine
        )["date_key"].tolist()

        new_dates = date_df[~date_df["date_key"].isin(existing_keys)]

        if not new_dates.empty:
            new_dates.drop(columns=["date"]).to_sql(
                "dim_date",
                engine,
                schema="ecom_dwh",
                if_exists="append",
                index=False,
            )
            print(f"Inserted {len(new_dates)} days into dim_date.")
        else:
            print("dim_date up to date.")
    else:
        print("WARNING: invoice_date range not found in staging; dim_date not updated.")

    # ---------------------------------------------------------
    # 2. Load Fact Table
    # ---------------------------------------------------------
    print("Loading Fact Sales...")

    execute_query(text("TRUNCATE TABLE ecom_dwh.fact_sales RESTART IDENTITY;"))

    # Debug count (safe)
    count_query = """
    SELECT COUNT(*)
    FROM ecom_staging.stg_sales s
    LEFT JOIN ecom_dwh.dim_product dp ON s.stock_code = dp.stock_code
    LEFT JOIN ecom_dwh.dim_customer dc
      ON COALESCE(NULLIF(TRIM(s.customer_id), ''), :unknown_id) = dc.customer_id
    LEFT JOIN ecom_dwh.dim_country dcn ON s.country = dcn.country_name
    """
    with engine.connect() as conn:
        pre_check = conn.execute(text(count_query), {"unknown_id": UNKNOWN_CUSTOMER}).scalar()
        print(f"DEBUG: Expecting to insert {pre_check} rows into fact_sales.")

    fact_query = """
    INSERT INTO ecom_dwh.fact_sales (
        date_key, product_key, customer_key, country_key,
        invoice_no, quantity, unit_price, revenue, is_cancelled
    )
    SELECT
        TO_CHAR(s.invoice_date, 'YYYYMMDD')::INT AS date_key,
        dp.product_key,
        dc.customer_key,
        dcn.country_key,
        s.invoice_no,
        s.quantity,
        s.unit_price,
        s.revenue,
        s.is_cancelled
    FROM ecom_staging.stg_sales s
    LEFT JOIN ecom_dwh.dim_product dp ON s.stock_code = dp.stock_code
    LEFT JOIN ecom_dwh.dim_customer dc
      ON COALESCE(NULLIF(TRIM(s.customer_id), ''), :unknown_id) = dc.customer_id
    LEFT JOIN ecom_dwh.dim_country dcn ON s.country = dcn.country_name;
    """

    with engine.connect() as conn:
        result = conn.execute(text(fact_query), {"unknown_id": UNKNOWN_CUSTOMER})
        conn.commit()
        print(f"Successfully loaded fact_sales. Rows Affected: {result.rowcount}")


if __name__ == "__main__":
    load_dwh()
