import ast
import hashlib
import unittest
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


ROOT = Path(__file__).resolve().parents[1]


def load_notebook_definitions(path, entrypoint, excluded_import=None):
    # Notebook modules execute their pipeline at import; tests load definitions only.
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    body = []
    for node in tree.body:
        if (
            isinstance(node, ast.Expr)
            and isinstance(node.value, ast.Call)
            and isinstance(node.value.func, ast.Name)
            and node.value.func.id == entrypoint
        ):
            continue
        if (
            excluded_import
            and isinstance(node, ast.ImportFrom)
            and node.module == excluded_import
        ):
            continue
        body.append(node)

    module = ast.fix_missing_locations(ast.Module(body=body, type_ignores=[]))
    namespace = {"__name__": f"test_{entrypoint}"}
    exec(compile(module, str(path), "exec"), namespace)
    return namespace


class RetailTransformationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("retail-transformation-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        cls.bronze = load_notebook_definitions(
            ROOT / "Bronze_Notebook.Notebook/notebook-content.py",
            "run_ingestion",
            excluded_import="delta.tables",
        )
        cls.silver_gold = load_notebook_definitions(
            ROOT / "Silver_Gold_Notebook.Notebook/notebook-content.py",
            "run_transformations",
        )
        cls.bronze["spark"] = cls.spark
        cls.silver_gold["spark"] = cls.spark

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_bronze_trims_strings_and_converts_blank_to_null(self):
        schema = StructType(
            [
                StructField("CustomerID", StringType(), True),
                StructField("City", StringType(), True),
            ]
        )
        frame = self.spark.createDataFrame(
            [(" C0001 ", "   ")],
            schema,
        )

        normalized = self.bronze["normalize_strings"](frame, schema).first()

        self.assertEqual(normalized.CustomerID, "C0001")
        self.assertIsNone(normalized.City)

    def test_bronze_rejects_null_and_duplicate_primary_keys(self):
        null_key = self.spark.createDataFrame(
            [("C0001",), (None,)], ["CustomerID"]
        )
        duplicate_key = self.spark.createDataFrame(
            [("C0001",), ("C0001",)], ["CustomerID"]
        )

        with self.assertRaisesRegex(ValueError, "contains null values"):
            self.bronze["validate_primary_key"](
                null_key, "CustomerID", "customers"
            )
        with self.assertRaisesRegex(ValueError, "duplicate values"):
            self.bronze["validate_primary_key"](
                duplicate_key, "CustomerID", "customers"
            )

    def test_bronze_rejects_invalid_sales_measures(self):
        schema = StructType(
            [
                StructField("Quantity", IntegerType(), False),
                StructField("UnitPrice", DecimalType(12, 2), False),
                StructField("LineTotal", DecimalType(12, 2), False),
            ]
        )
        invalid_sale = self.spark.createDataFrame(
            [(0, Decimal("10.00"), Decimal("0.00"))],
            schema,
        )

        with self.assertRaisesRegex(
            ValueError, "Quantity must be greater than zero"
        ):
            self.bronze["validate_business_rules"](
                invalid_sale, "sales_transactions"
            )

    def test_silver_quarantines_missing_required_values_and_deduplicates(self):
        schema = StructType(
            [
                StructField("CustomerID", StringType(), True),
                StructField("CustomerSegment", StringType(), True),
                StructField("City", StringType(), True),
                StructField("Region", StringType(), True),
                StructField("SignupDate", DateType(), True),
                StructField("ingestion_timestamp", TimestampType(), True),
                StructField("source_file", StringType(), True),
            ]
        )
        rows = [
            (
                "c0001",
                "Consumer",
                "WESTVALE",
                "WEST",
                date(2022, 1, 26),
                datetime(2024, 1, 1, 0, 0, 0),
                "customers.csv",
            ),
            (
                "C0001",
                "Small   Business",
                "MEADOWRIDGE",
                "SOUTH",
                date(2023, 11, 8),
                datetime(2024, 1, 2, 0, 0, 0),
                "customers.csv",
            ),
            (
                "C0002",
                None,
                "NORTHHAVEN",
                "NORTH",
                date(2023, 1, 1),
                datetime(2024, 1, 3, 0, 0, 0),
                "customers.csv",
            ),
        ]
        self.spark.createDataFrame(rows, schema).createOrReplaceTempView("customers")
        try:
            cleaned, quarantines = self.silver_gold["clean_and_deduplicate"](
                "customers", self.silver_gold["DATASETS"]["customers"]
            )

            self.assertEqual(cleaned.count(), 1)
            retained = cleaned.first()
            self.assertEqual(retained.CustomerID, "C0001")
            self.assertEqual(retained.CustomerSegment, "Small Business")
            self.assertEqual(retained.City, "Meadowridge")

            rejected = [row for frame in quarantines for row in frame.collect()]
            self.assertEqual(len(rejected), 2)
            reasons = [row.RejectionReason for row in rejected]
            self.assertTrue(any("CustomerSegment is required" in reason for reason in reasons))
            self.assertTrue(any("duplicate business key" in reason for reason in reasons))
        finally:
            self.spark.catalog.dropTempView("customers")

    def test_sales_with_orphan_dimension_keys_are_quarantined(self):
        schema = StructType(
            [
                StructField("TransactionID", StringType(), False),
                StructField("TransactionDate", DateType(), False),
                StructField("CustomerID", StringType(), False),
                StructField("ProductID", StringType(), False),
                StructField("StoreID", StringType(), False),
                StructField("Quantity", IntegerType(), False),
                StructField("UnitPrice", DecimalType(12, 2), False),
                StructField("LineTotal", DecimalType(12, 2), False),
            ]
        )
        sales = self.spark.createDataFrame(
            [
                (
                    "T0001",
                    date(2024, 1, 1),
                    "C0001",
                    "P0001",
                    "S001",
                    2,
                    Decimal("12.50"),
                    Decimal("25.00"),
                ),
                (
                    "T0002",
                    date(2024, 1, 2),
                    "C9999",
                    "P9999",
                    "S999",
                    1,
                    Decimal("5.00"),
                    Decimal("5.00"),
                ),
            ],
            schema,
        )
        customers = self.spark.createDataFrame([("C0001",)], ["CustomerID"])
        products = self.spark.createDataFrame([("P0001",)], ["ProductID"])
        stores = self.spark.createDataFrame([("S001",)], ["StoreID"])

        valid, quarantined = self.silver_gold["reject_orphan_sales"](
            sales, customers, products, stores
        )

        self.assertEqual(
            [row.TransactionID for row in valid.select("TransactionID").collect()],
            ["T0001"],
        )
        rejected = quarantined.first()
        self.assertEqual(rejected.BusinessKey, "T0002")
        self.assertIn("unknown CustomerID", rejected.RejectionReason)
        self.assertIn("unknown ProductID", rejected.RejectionReason)
        self.assertIn("unknown StoreID", rejected.RejectionReason)

    def test_surrogate_keys_and_date_dimension_match_gold_contract(self):
        customers = self.spark.createDataFrame([("C0001",)], ["CustomerID"])
        customer_dim = self.silver_gold["make_surrogate_dimension"](
            customers,
            "CustomerID",
            "CustomerKey",
            "customer",
            ["CustomerID"],
        )
        expected_key = hashlib.sha256(b"customer|C0001").hexdigest()
        self.assertEqual(customer_dim.first().CustomerKey, expected_key)

        sales = self.spark.createDataFrame(
            [(date(2024, 1, 1),), (date(2024, 1, 6),)],
            ["TransactionDate"],
        )
        dates = self.silver_gold["make_date_dimension"](sales)
        date_rows = {row.DateKey: row for row in dates.collect()}

        self.assertEqual(len(date_rows), 6)
        self.assertEqual(date_rows[20240101].DayOfWeek, 2)
        self.assertFalse(date_rows[20240101].IsWeekend)
        self.assertTrue(date_rows[20240106].IsWeekend)

    def test_fact_sales_joins_dimensions_and_preserves_measures(self):
        sales_schema = StructType(
            [
                StructField("TransactionID", StringType(), False),
                StructField("TransactionDate", DateType(), False),
                StructField("CustomerID", StringType(), False),
                StructField("ProductID", StringType(), False),
                StructField("StoreID", StringType(), False),
                StructField("Quantity", IntegerType(), False),
                StructField("UnitPrice", DecimalType(12, 2), False),
                StructField("LineTotal", DecimalType(12, 2), False),
            ]
        )
        sales = self.spark.createDataFrame(
            [
                (
                    "T0001",
                    date(2024, 1, 1),
                    "C0001",
                    "P0001",
                    "S001",
                    2,
                    Decimal("12.50"),
                    Decimal("25.00"),
                )
            ],
            sales_schema,
        )

        def dimension(source_key, surrogate_key, prefix):
            source = self.spark.createDataFrame(
                [(sales.first()[source_key],)], [source_key]
            )
            return self.silver_gold["make_surrogate_dimension"](
                source,
                source_key,
                surrogate_key,
                prefix,
                [source_key],
            )

        customer_dim = dimension("CustomerID", "CustomerKey", "customer")
        product_dim = dimension("ProductID", "ProductKey", "product")
        store_dim = dimension("StoreID", "StoreKey", "store")
        date_dim = self.silver_gold["make_date_dimension"](sales)

        fact = self.silver_gold["make_fact_sales"](
            sales, customer_dim, product_dim, store_dim, date_dim
        ).first()

        self.assertEqual(
            fact.FactSalesKey,
            hashlib.sha256(b"sale|T0001").hexdigest(),
        )
        self.assertEqual(fact.DateKey, 20240101)
        self.assertEqual(fact.Quantity, 2)
        self.assertEqual(fact.UnitPrice, Decimal("12.50"))
        self.assertEqual(fact.LineTotal, Decimal("25.00"))


if __name__ == "__main__":
    unittest.main()
