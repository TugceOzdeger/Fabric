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
# as run context; neither parameter filters or routes the ingestion.
processing_date = ""
environment = "dev"

# CELL ********************

"""
Bronze ingestion for the synthetic retail sample.

Assumptions:
- Attach the target Lakehouse to this notebook as its default Lakehouse.
- Copy/upload the repository's data/retail-sales/*.csv files to that Lakehouse's
  Files/retail-sales/ directory. A Git checkout path is not Spark-accessible.
- The notebook creates or upserts managed Delta tables in the attached Lakehouse.
- Reprocessing the same source files upserts by each dataset's primary key; it
  does not delete target rows that are absent from a later source file.
- processing_date is run context only; environment is a deployment label only.
"""

import logging
from datetime import date, datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


LOGGER = logging.getLogger("retail_bronze_ingestion")
LOGGER.setLevel(logging.INFO)
if not LOGGER.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    LOGGER.addHandler(handler)

SOURCE_DIRECTORY = "Files/retail-sales"
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


DATASETS = [
    {
        "file_name": "customers.csv",
        "table_name": "customers",
        "primary_key": "CustomerID",
        "required_columns": ["CustomerID", "CustomerSegment", "SignupDate"],
        "schema": StructType(
            [
                StructField("CustomerID", StringType(), True),
                StructField("CustomerSegment", StringType(), True),
                StructField("City", StringType(), True),
                StructField("Region", StringType(), True),
                StructField("SignupDate", DateType(), True),
            ]
        ),
    },
    {
        "file_name": "products.csv",
        "table_name": "products",
        "primary_key": "ProductID",
        "required_columns": [
            "ProductID",
            "ProductName",
            "Category",
            "UnitPrice",
            "UnitCost",
        ],
        "schema": StructType(
            [
                StructField("ProductID", StringType(), True),
                StructField("ProductName", StringType(), True),
                StructField("Category", StringType(), True),
                StructField("UnitPrice", MONEY, True),
                StructField("UnitCost", MONEY, True),
            ]
        ),
    },
    {
        "file_name": "stores.csv",
        "table_name": "stores",
        "primary_key": "StoreID",
        "required_columns": [
            "StoreID",
            "StoreName",
            "City",
            "Region",
            "StoreType",
        ],
        "schema": StructType(
            [
                StructField("StoreID", StringType(), True),
                StructField("StoreName", StringType(), True),
                StructField("City", StringType(), True),
                StructField("Region", StringType(), True),
                StructField("StoreType", StringType(), True),
            ]
        ),
    },
    {
        "file_name": "sales-transactions.csv",
        "table_name": "sales_transactions",
        "primary_key": "TransactionID",
        "required_columns": [
            "TransactionID",
            "TransactionDate",
            "CustomerID",
            "ProductID",
            "StoreID",
            "Quantity",
            "UnitPrice",
            "LineTotal",
        ],
        "schema": StructType(
            [
                StructField("TransactionID", StringType(), True),
                StructField("TransactionDate", DateType(), True),
                StructField("CustomerID", StringType(), True),
                StructField("ProductID", StringType(), True),
                StructField("StoreID", StringType(), True),
                StructField("Quantity", IntegerType(), True),
                StructField("UnitPrice", MONEY, True),
                StructField("LineTotal", MONEY, True),
            ]
        ),
    },
]


def fail_if_any(frame, condition, message):
    invalid_count = frame.filter(condition).limit(1).count()
    if invalid_count:
        raise ValueError(message)


def validate_primary_key(frame, primary_key, table_name):
    fail_if_any(
        frame,
        F.col(primary_key).isNull(),
        f"{table_name}: primary key {primary_key} contains null values",
    )
    duplicate_count = (
        frame.groupBy(primary_key)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicate_count:
        raise ValueError(f"{table_name}: duplicate values in primary key {primary_key}")


def validate_required_columns(frame, columns, table_name):
    for column in columns:
        fail_if_any(
            frame,
            F.col(column).isNull(),
            f"{table_name}: required column {column} contains missing or invalid values",
        )


def validate_business_rules(frame, table_name):
    rules = {
        "products": [
            ("UnitPrice must be greater than zero", F.col("UnitPrice") <= 0),
            ("UnitCost must be zero or greater", F.col("UnitCost") < 0),
        ],
        "sales_transactions": [
            ("Quantity must be greater than zero", F.col("Quantity") <= 0),
            ("UnitPrice must be greater than zero", F.col("UnitPrice") <= 0),
            ("LineTotal must equal Quantity times UnitPrice", F.col("LineTotal") != F.round(F.col("UnitPrice") * F.col("Quantity"), 2)),
        ],
    }
    for message, condition in rules.get(table_name, []):
        fail_if_any(frame, condition, f"{table_name}: {message}")


def normalize_strings(frame, schema):
    for field in schema.fields:
        if isinstance(field.dataType, StringType):
            trimmed = F.trim(F.col(field.name))
            frame = frame.withColumn(
                field.name,
                F.when(trimmed == "", F.lit(None).cast(StringType())).otherwise(trimmed),
            )
    return frame


def upsert_delta_table(frame, table_name, primary_key):
    if not spark.catalog.tableExists(table_name):
        frame.write.format("delta").mode("error").saveAsTable(table_name)
        LOGGER.info("Created managed Delta table %s", table_name)
    else:
        delta_table = DeltaTable.forName(spark, table_name)
        (
            delta_table.alias("target")
            .merge(
                frame.alias("source"),
                f"target.`{primary_key}` = source.`{primary_key}`",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        LOGGER.info("Upserted staged rows into %s", table_name)

    target = spark.table(table_name)
    validate_primary_key(target, primary_key, table_name)
    return target.count()


def ingest_dataset(dataset):
    file_name = dataset["file_name"]
    table_name = dataset["table_name"]
    source_path = f"{SOURCE_DIRECTORY}/{file_name}"
    LOGGER.info("Reading %s", source_path)

    staged = (
        spark.read.option("header", True)
        .option("mode", "FAILFAST")
        .option("dateFormat", "yyyy-MM-dd")
        .schema(dataset["schema"])
        .csv(source_path)
    )
    staged = normalize_strings(staged, dataset["schema"])
    staged = (
        staged.withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_file", F.lit(file_name))
    )

    input_count = staged.count()
    if input_count == 0:
        raise ValueError(f"{source_path}: source file contains no data rows")

    validate_required_columns(staged, dataset["required_columns"], table_name)
    validate_primary_key(staged, dataset["primary_key"], table_name)
    validate_business_rules(staged, table_name)

    target_count = upsert_delta_table(
        staged, table_name, dataset["primary_key"]
    )
    LOGGER.info(
        "Completed %s: input_rows=%d target_rows=%d",
        table_name,
        input_count,
        target_count,
    )


def validate_sales_foreign_keys():
    sales = spark.table("sales_transactions")
    references = [
        ("CustomerID", "customers"),
        ("ProductID", "products"),
        ("StoreID", "stores"),
    ]
    for foreign_key, dimension_table in references:
        sales_keys = sales.select(F.col(foreign_key).alias("foreign_key")).distinct()
        dimension_keys = (
            spark.table(dimension_table)
            .select(F.col(foreign_key).alias("foreign_key"))
            .distinct()
        )
        missing_count = (
            sales_keys.join(dimension_keys, on="foreign_key", how="left_anti").count()
        )
        if missing_count:
            raise ValueError(
                f"sales_transactions: {missing_count} {foreign_key} values "
                f"do not match {dimension_table}"
            )
        LOGGER.info(
            "Validated sales_transactions.%s against %s",
            foreign_key,
            dimension_table,
        )


def run_ingestion():
    try:
        log_run_context()
        for dataset in DATASETS:
            ingest_dataset(dataset)
        validate_sales_foreign_keys()
        LOGGER.info("Retail Bronze ingestion completed successfully")
    except Exception:
        LOGGER.exception("Retail Bronze ingestion failed")
        raise


run_ingestion()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
