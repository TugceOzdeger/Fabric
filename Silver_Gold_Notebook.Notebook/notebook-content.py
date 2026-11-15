# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Pipeline parameters. An empty processing_date means use the current UTC date
# as run context; neither parameter filters or routes the transformation.
processing_date = ""
environment = "dev"

# METADATA ********************

# META {
# META   "tags": [
# META     "parameters"
# META   ]
# META }

# CELL ********************

"""
Silver cleaning and Gold dimensional model for the retail sample.

Assumptions:
- Attach the Lakehouse containing the Bronze tables as this notebook's default
  Lakehouse. Bronze tables are customers, products, stores, and sales_transactions.
- Silver tables are cleaned snapshots. Invalid rows and duplicate business keys
  are written to silver_rejected_records with a reason and serialized source row.
- Gold dimensions use Type 1 semantics: each run rebuilds the current snapshot;
  no historical attribute versions are retained.
- Gold tables are managed Delta tables. Silver and Gold outputs are overwritten
  from Bronze on each run; surrogate keys are deterministic hashes of business
  keys, so they remain stable across reruns and joins.
- processing_date is run context only; environment is a deployment label only.
"""

import logging
from functools import reduce
from datetime import date, datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType, StringType
from pyspark.sql.window import Window


LOGGER = logging.getLogger("retail_silver_gold")
LOGGER.setLevel(logging.INFO)
if not LOGGER.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    LOGGER.addHandler(handler)

MONEY = DecimalType(12, 2)


def log_run_context():
    selected_environment = environment.strip()
    if not selected_environment:
        raise ValueError("environment must be a non-empty deployment label")

    requested_date = processing_date.strip()
    if requested_date:
        try:
            parsed_date = date.fromisoformat(requested_date)
        except ValueError as exc:
            raise ValueError("processing_date must use ISO YYYY-MM-DD format") from exc
        if parsed_date.isoformat() != requested_date:
            raise ValueError("processing_date must use ISO YYYY-MM-DD format")
        effective_date = parsed_date.isoformat()
    else:
        effective_date = datetime.now(timezone.utc).date().isoformat()

    LOGGER.info(
        "Run context: processing_date=%s environment=%s",
        effective_date,
        selected_environment,
    )


DATASETS = {
    "customers": {
        "primary_key": "CustomerID",
        "required": ["CustomerID", "CustomerSegment", "SignupDate"],
        "uppercase": ["CustomerID"],
        "title_case": ["CustomerSegment", "City", "Region"],
        "casts": {"SignupDate": "date"},
        "checks": [
            (
                "unsupported CustomerSegment",
                lambda: ~F.col("CustomerSegment").isin(
                    "Consumer", "Small Business", "Enterprise"
                ),
            )
        ],
    },
    "products": {
        "primary_key": "ProductID",
        "required": ["ProductID", "ProductName", "Category", "UnitPrice", "UnitCost"],
        "uppercase": ["ProductID"],
        "title_case": ["Category"],
        "casts": {"UnitPrice": MONEY, "UnitCost": MONEY},
        "checks": [
            ("UnitPrice must be greater than zero", lambda: F.col("UnitPrice") <= 0),
            ("UnitCost must be zero or greater", lambda: F.col("UnitCost") < 0),
        ],
    },
    "stores": {
        "primary_key": "StoreID",
        "required": ["StoreID", "StoreName", "City", "Region", "StoreType"],
        "uppercase": ["StoreID"],
        "title_case": ["City", "Region", "StoreType"],
        "casts": {},
        "checks": [],
    },
    "sales_transactions": {
        "primary_key": "TransactionID",
        "required": [
            "TransactionID",
            "TransactionDate",
            "CustomerID",
            "ProductID",
            "StoreID",
            "Quantity",
            "UnitPrice",
            "LineTotal",
        ],
        "uppercase": ["TransactionID", "CustomerID", "ProductID", "StoreID"],
        "title_case": [],
        "casts": {
            "TransactionDate": "date",
            "Quantity": IntegerType(),
            "UnitPrice": MONEY,
            "LineTotal": MONEY,
        },
        "checks": [
            ("Quantity must be greater than zero", lambda: F.col("Quantity") <= 0),
            ("UnitPrice must be greater than zero", lambda: F.col("UnitPrice") <= 0),
            (
                "LineTotal must equal Quantity times UnitPrice",
                lambda: F.col("LineTotal")
                != F.round(F.col("UnitPrice") * F.col("Quantity"), 2),
            ),
        ],
    },
}


def fail_if_any(frame, condition, message):
    if frame.filter(condition).limit(1).count():
        raise ValueError(message)


def validate_unique_key(frame, key, label, allow_empty=False):
    row_count = frame.count()
    if row_count == 0 and not allow_empty:
        raise ValueError(f"{label}: output is empty")
    fail_if_any(frame, F.col(key).isNull(), f"{label}: null {key} values found")
    duplicate_count = (
        frame.groupBy(key).count().filter(F.col("count") > 1).limit(1).count()
    )
    if duplicate_count:
        raise ValueError(f"{label}: duplicate {key} values found")
    LOGGER.info("%s quality check passed: rows=%d unique_key=%s", label, row_count, key)
    return row_count


def normalize_frame(frame, dataset):
    data_types = {field.name: field.dataType for field in frame.schema.fields}
    for column in frame.columns:
        if isinstance(data_types[column], StringType):
            trimmed = F.regexp_replace(F.trim(F.col(column)), r"\s+", " ")
            frame = frame.withColumn(
                column,
                F.when(trimmed == "", F.lit(None).cast("string")).otherwise(trimmed),
            )
    for column in dataset["uppercase"]:
        frame = frame.withColumn(column, F.upper(F.col(column)))
    for column in dataset["title_case"]:
        frame = frame.withColumn(column, F.initcap(F.col(column)))
    for column, data_type in dataset["casts"].items():
        frame = frame.withColumn(column, F.col(column).cast(data_type))
    return frame


def quarantine_frame(frame, table_name, key, reason):
    record_columns = [column for column in frame.columns if not column.startswith("__")]
    reason_column = F.lit(reason) if isinstance(reason, str) else reason
    return frame.select(
        F.lit(table_name).alias("SourceTable"),
        F.col(key).cast("string").alias("BusinessKey"),
        reason_column.alias("RejectionReason"),
        F.to_json(
            F.struct(*[F.col(column).alias(column) for column in record_columns])
        ).alias("RecordJson"),
        F.current_timestamp().alias("QuarantinedAt"),
    )


def clean_and_deduplicate(table_name, dataset):
    frame = spark.table(table_name)
    expected = set(dataset["required"]) | {
        "ingestion_timestamp",
        "source_file",
    }
    missing_columns = sorted(expected - set(frame.columns))
    if missing_columns:
        raise ValueError(
            f"{table_name}: Bronze table is missing columns {missing_columns}"
        )

    source_count = frame.count()
    if source_count == 0:
        raise ValueError(f"{table_name}: Bronze table contains no rows")

    frame = normalize_frame(frame, dataset)
    required_conditions = [
        (f"{column} is required", F.col(column).isNull())
        for column in dataset["required"]
    ]
    checks = required_conditions + [
        (message, condition())
        for message, condition in dataset["checks"]
    ]
    frame = frame.withColumn(
        "__rejection_reason",
        F.concat_ws(
            "; ",
            *[
                F.when(condition, F.lit(message))
                for message, condition in checks
            ],
        ),
    )

    invalid = frame.filter(F.length(F.col("__rejection_reason")) > 0)
    invalid_count = invalid.count()
    quarantine = [
        quarantine_frame(
            invalid,
            table_name,
            dataset["primary_key"],
            F.col("__rejection_reason"),
        )
    ]

    valid = frame.filter(F.length(F.col("__rejection_reason")) == 0)
    payload_columns = [
        column for column in valid.columns if not column.startswith("__")
    ]
    valid = valid.withColumn(
        "__row_hash",
        F.sha2(
            F.to_json(
                F.struct(*[F.col(column).alias(column) for column in payload_columns])
            ),
            256,
        ),
    )
    window = Window.partitionBy(dataset["primary_key"]).orderBy(
        F.col("ingestion_timestamp").desc_nulls_last(),
        F.col("__row_hash").asc(),
    )
    ranked = valid.withColumn("__duplicate_rank", F.row_number().over(window))
    duplicates = ranked.filter(F.col("__duplicate_rank") > 1)
    duplicate_count = duplicates.count()
    quarantine.append(
        quarantine_frame(
            duplicates,
            table_name,
            dataset["primary_key"],
            "duplicate business key; newest deterministic row retained",
        )
    )

    cleaned = ranked.filter(F.col("__duplicate_rank") == 1).drop(
        "__rejection_reason", "__row_hash", "__duplicate_rank"
    )
    clean_count = validate_unique_key(
        cleaned, dataset["primary_key"], f"silver_{table_name}"
    )
    reason_counts = [
        row.asDict()
        for row in invalid.groupBy("__rejection_reason").count().collect()
    ]
    LOGGER.info(
        "%s cleaning: source=%d invalid=%d duplicate=%d clean=%d reasons=%s",
        table_name,
        source_count,
        invalid_count,
        duplicate_count,
        clean_count,
        reason_counts,
    )
    return cleaned, quarantine


def reject_orphan_sales(sales, customers, products, stores):
    joined = (
        sales.alias("s")
        .join(
            customers.select(F.col("CustomerID").alias("__customer_id")).alias("c"),
            F.col("s.CustomerID") == F.col("c.__customer_id"),
            "left",
        )
        .join(
            products.select(F.col("ProductID").alias("__product_id")).alias("p"),
            F.col("s.ProductID") == F.col("p.__product_id"),
            "left",
        )
        .join(
            stores.select(F.col("StoreID").alias("__store_id")).alias("st"),
            F.col("s.StoreID") == F.col("st.__store_id"),
            "left",
        )
    )
    reasons = F.concat_ws(
        "; ",
        F.when(F.col("c.__customer_id").isNull(), F.lit("unknown CustomerID")),
        F.when(F.col("p.__product_id").isNull(), F.lit("unknown ProductID")),
        F.when(F.col("st.__store_id").isNull(), F.lit("unknown StoreID")),
    )
    invalid = joined.filter(F.length(reasons) > 0).select(
        "s.*", reasons.alias("__rejection_reason")
    )
    valid = joined.filter(F.length(reasons) == 0).select("s.*")
    orphan_count = invalid.count()
    LOGGER.info(
        "sales_transactions foreign-key check: valid=%d orphan=%d",
        valid.count(),
        orphan_count,
    )
    return valid, quarantine_frame(
        invalid,
        "sales_transactions",
        "TransactionID",
        F.col("__rejection_reason"),
    )


def write_snapshot(frame, table_name, primary_key=None, allow_empty=False):
    row_count = frame.count()
    if row_count == 0 and not allow_empty:
        raise ValueError(f"{table_name}: refusing to write an empty snapshot")
    (
        frame.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
    written = spark.table(table_name)
    if primary_key:
        written_count = validate_unique_key(
            written, primary_key, table_name, allow_empty=allow_empty
        )
    else:
        written_count = written.count()
    if written_count != row_count:
        raise ValueError(
            f"{table_name}: written row count {written_count} "
            f"does not match staged count {row_count}"
        )
    LOGGER.info("Wrote managed Delta snapshot %s: rows=%d", table_name, row_count)


def make_surrogate_dimension(frame, natural_key, surrogate_key, prefix, columns):
    return (
        frame.select(*columns)
        .withColumn(
            surrogate_key,
            F.sha2(
                F.concat(F.lit(f"{prefix}|"), F.col(natural_key)),
                256,
            ),
        )
        .select(surrogate_key, *columns)
    )


def make_date_dimension(sales):
    bounds = sales.agg(
        F.min("TransactionDate").alias("min_date"),
        F.max("TransactionDate").alias("max_date"),
    ).first()
    if bounds["min_date"] is None or bounds["max_date"] is None:
        raise ValueError("Cannot build dim_date without valid sales dates")
    dates = spark.range(1).select(
        F.explode(
            F.sequence(F.lit(bounds["min_date"]), F.lit(bounds["max_date"]))
        ).alias("Date")
    )
    return dates.select(
        F.date_format("Date", "yyyyMMdd").cast("int").alias("DateKey"),
        "Date",
        F.year("Date").alias("Year"),
        F.quarter("Date").alias("Quarter"),
        F.month("Date").alias("Month"),
        F.date_format("Date", "MMMM").alias("MonthName"),
        F.dayofmonth("Date").alias("DayOfMonth"),
        F.dayofweek("Date").alias("DayOfWeek"),
        F.date_format("Date", "EEEE").alias("DayName"),
        F.dayofweek("Date").isin(1, 7).alias("IsWeekend"),
    )


def make_fact_sales(sales, customers, products, stores, dates):
    return (
        sales.alias("s")
        .join(
            customers.select("CustomerID", "CustomerKey").alias("c"),
            F.col("s.CustomerID") == F.col("c.CustomerID"),
            "inner",
        )
        .join(
            products.select("ProductID", "ProductKey").alias("p"),
            F.col("s.ProductID") == F.col("p.ProductID"),
            "inner",
        )
        .join(
            stores.select("StoreID", "StoreKey").alias("st"),
            F.col("s.StoreID") == F.col("st.StoreID"),
            "inner",
        )
        .join(
            dates.select("Date", "DateKey").alias("d"),
            F.col("s.TransactionDate") == F.col("d.Date"),
            "inner",
        )
        .select(
            F.sha2(F.concat(F.lit("sale|"), F.col("s.TransactionID")), 256).alias(
                "FactSalesKey"
            ),
            F.col("s.TransactionID").alias("TransactionID"),
            F.col("d.DateKey").alias("DateKey"),
            F.col("c.CustomerKey").alias("CustomerKey"),
            F.col("p.ProductKey").alias("ProductKey"),
            F.col("st.StoreKey").alias("StoreKey"),
            F.col("s.Quantity").alias("Quantity"),
            F.col("s.UnitPrice").alias("UnitPrice"),
            F.col("s.LineTotal").alias("LineTotal"),
        )
    )


def validate_fact_references(fact, dimension, foreign_key, dimension_key):
    unmatched = (
        fact.select(F.col(foreign_key).alias("__foreign_key"))
        .distinct()
        .join(
            dimension.select(F.col(dimension_key).alias("__dimension_key")).distinct(),
            F.col("__foreign_key") == F.col("__dimension_key"),
            "left_anti",
        )
        .count()
    )
    if unmatched:
        raise ValueError(
            f"fact_sales: {unmatched} unmatched {foreign_key} references"
        )
    fail_if_any(
        fact,
        F.col(foreign_key).isNull(),
        f"fact_sales: null {foreign_key} values found",
    )
    LOGGER.info("fact_sales foreign-key check passed: %s", foreign_key)


def run_transformations():
    try:
        log_run_context()
        silver = {}
        quarantine_frames = []
        for table_name, dataset in DATASETS.items():
            silver[table_name], rejected = clean_and_deduplicate(
                table_name, dataset
            )
            quarantine_frames.extend(rejected)

        silver["sales_transactions"], orphan_quarantine = reject_orphan_sales(
            silver["sales_transactions"],
            silver["customers"],
            silver["products"],
            silver["stores"],
        )
        quarantine_frames.append(orphan_quarantine)
        validate_unique_key(
            silver["sales_transactions"],
            "TransactionID",
            "silver_sales_transactions",
        )

        for table_name, frame in silver.items():
            write_snapshot(
                frame,
                f"silver_{table_name}",
                DATASETS[table_name]["primary_key"],
            )

        rejected_records = reduce(
            lambda left, right: left.unionByName(right), quarantine_frames
        )
        write_snapshot(
            rejected_records,
            "silver_rejected_records",
            allow_empty=True,
        )

        dim_customer = make_surrogate_dimension(
            silver["customers"],
            "CustomerID",
            "CustomerKey",
            "customer",
            [
                "CustomerID",
                "CustomerSegment",
                "City",
                "Region",
                "SignupDate",
            ],
        )
        dim_product = make_surrogate_dimension(
            silver["products"],
            "ProductID",
            "ProductKey",
            "product",
            ["ProductID", "ProductName", "Category", "UnitPrice", "UnitCost"],
        )
        dim_store = make_surrogate_dimension(
            silver["stores"],
            "StoreID",
            "StoreKey",
            "store",
            ["StoreID", "StoreName", "City", "Region", "StoreType"],
        )
        dim_date = make_date_dimension(silver["sales_transactions"])

        for frame, surrogate_key, name in [
            (dim_customer, "CustomerKey", "dim_customer"),
            (dim_product, "ProductKey", "dim_product"),
            (dim_store, "StoreKey", "dim_store"),
            (dim_date, "DateKey", "dim_date"),
        ]:
            validate_unique_key(frame, surrogate_key, name)
            write_snapshot(frame, name, surrogate_key)

        fact_sales = make_fact_sales(
            silver["sales_transactions"],
            dim_customer,
            dim_product,
            dim_store,
            dim_date,
        )
        expected_fact_count = silver["sales_transactions"].count()
        fact_count = validate_unique_key(
            fact_sales, "FactSalesKey", "fact_sales"
        )
        if fact_count != expected_fact_count:
            raise ValueError(
                f"fact_sales: expected {expected_fact_count} rows, got {fact_count}"
            )
        validate_fact_references(
            fact_sales, dim_customer, "CustomerKey", "CustomerKey"
        )
        validate_fact_references(fact_sales, dim_product, "ProductKey", "ProductKey")
        validate_fact_references(fact_sales, dim_store, "StoreKey", "StoreKey")
        validate_fact_references(fact_sales, dim_date, "DateKey", "DateKey")
        write_snapshot(fact_sales, "fact_sales", "FactSalesKey")
        LOGGER.info("Silver and Gold transformations completed successfully")
    except Exception:
        LOGGER.exception("Silver and Gold transformations failed")
        raise


run_transformations()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
