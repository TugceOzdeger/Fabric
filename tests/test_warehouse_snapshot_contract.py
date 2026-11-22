import re
import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "Bronze_Warehouse.Warehouse" / "scripts"
TABLE_COLUMNS = {
    "dim_customer": [
        "CustomerKey",
        "CustomerID",
        "CustomerSegment",
        "City",
        "Region",
        "SignupDate",
    ],
    "dim_product": [
        "ProductKey",
        "ProductID",
        "ProductName",
        "Category",
        "UnitPrice",
        "UnitCost",
    ],
    "dim_store": [
        "StoreKey",
        "StoreID",
        "StoreName",
        "City",
        "Region",
        "StoreType",
    ],
    "dim_date": [
        "DateKey",
        "Date",
        "Year",
        "Quarter",
        "Month",
        "MonthName",
        "DayOfMonth",
        "DayOfWeek",
        "DayName",
        "IsWeekend",
    ],
    "fact_sales": [
        "FactSalesKey",
        "TransactionID",
        "DateKey",
        "CustomerKey",
        "ProductKey",
        "StoreKey",
        "Quantity",
        "UnitPrice",
        "LineTotal",
    ],
}


def create_table_columns(sql, table_name):
    match = re.search(
        rf"CREATE TABLE dbo\.{re.escape(table_name)}\s*\((.*?)\n\s*\);",
        sql,
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        raise AssertionError(f"CREATE TABLE for {table_name} not found")
    columns = []
    for line in match.group(1).splitlines():
        match = re.match(r"\s*\[?([A-Za-z][A-Za-z0-9_]*)\]?\s+", line)
        if match:
            columns.append(match.group(1))
    return columns


def column_list(text):
    return [column.strip().strip("[]") for column in text.split(",")]


class WarehouseSnapshotContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.staging_sql = (SCRIPTS / "05_create_snapshot_staging.sql").read_text(
            encoding="utf-8"
        )
        cls.publish_sql = (SCRIPTS / "06_publish_snapshot.sql").read_text(
            encoding="utf-8"
        )

    def test_staging_and_target_table_columns_match(self):
        dimensions_sql = (SCRIPTS / "01_dimensions.sql").read_text(encoding="utf-8")
        fact_sql = (SCRIPTS / "02_fact_sales.sql").read_text(encoding="utf-8")
        target_sql = dimensions_sql + "\n" + fact_sql

        for table_name, expected_columns in TABLE_COLUMNS.items():
            with self.subTest(table=table_name):
                self.assertEqual(
                    create_table_columns(self.staging_sql, f"stg_{table_name}"),
                    expected_columns,
                )
                self.assertEqual(
                    create_table_columns(target_sql, table_name),
                    expected_columns,
                )

    def test_publish_maps_every_snapshot_table_and_column_in_order(self):
        for table_name, expected_columns in TABLE_COLUMNS.items():
            with self.subTest(table=table_name):
                match = re.search(
                    rf"INSERT INTO dbo\.{re.escape(table_name)}\s*"
                    r"\((.*?)\)\s*SELECT\s+(.*?)\s+"
                    rf"FROM dbo\.stg_{re.escape(table_name)}\s*;",
                    self.publish_sql,
                    re.IGNORECASE | re.DOTALL,
                )
                self.assertIsNotNone(match)
                self.assertEqual(column_list(match.group(1)), expected_columns)
                self.assertEqual(column_list(match.group(2)), expected_columns)

    def test_publish_validates_before_transaction_and_rolls_back_failures(self):
        first_validation = self.publish_sql.index("IF NOT EXISTS")
        transaction = self.publish_sql.index("BEGIN TRANSACTION")
        self.assertLess(first_validation, transaction)
        self.assertIn("BEGIN CATCH", self.publish_sql)
        self.assertIn("ROLLBACK TRANSACTION", self.publish_sql)
        self.assertIn("COMMIT TRANSACTION", self.publish_sql)
        self.assertEqual(
            len(re.findall(r"^\s*;THROW 51000,", self.publish_sql, re.MULTILINE)),
            18,
        )

    def test_pipeline_loads_gold_snapshots_and_has_no_unbound_script_activity(self):
        pipeline = json.loads(
            (ROOT / "Bronze_Pipeline.DataPipeline/pipeline-content.json").read_text(
                encoding="utf-8"
            )
        )["properties"]
        activities = pipeline["activities"]
        by_name = {activity["name"]: activity for activity in activities}
        gold_tables = list(TABLE_COLUMNS)

        self.assertEqual(len(activities), 7)
        self.assertEqual(activities[0]["name"], "BronzeIngestion")
        self.assertEqual(activities[0]["typeProperties"]["workspaceId"],
                         "0469cb50-a313-4569-8273-f396841183c7")
        self.assertEqual(
            activities[1]["dependsOn"],
            [{"activity": "BronzeIngestion", "dependencyConditions": ["Succeeded"]}],
        )
        self.assertEqual(
            activities[1]["typeProperties"]["workspaceId"],
            "0469cb50-a313-4569-8273-f396841183c7",
        )

        copy_names = [f"Copy Gold {table}" for table in gold_tables]
        for table_name, activity_name in zip(gold_tables, copy_names):
            with self.subTest(table=table_name):
                copy_activity = by_name[activity_name]
                self.assertEqual(copy_activity["type"], "Copy")
                self.assertEqual(
                    copy_activity["dependsOn"],
                    [
                        {
                            "activity": "SilverGoldTransformation",
                            "dependencyConditions": ["Succeeded"],
                        }
                    ],
                )
                properties = copy_activity["typeProperties"]
                source = properties["source"]
                source_dataset = source["datasetSettings"]
                self.assertEqual(source["type"], "LakehouseTableSource")
                self.assertEqual(source_dataset["type"], "LakehouseTable")
                self.assertEqual(source_dataset["typeProperties"]["schema"], "dbo")
                self.assertEqual(
                    source_dataset["typeProperties"]["table"], table_name
                )
                self.assertEqual(
                    source_dataset["linkedService"]["properties"]["typeProperties"],
                    {
                        "workspaceId": "0469cb50-a313-4569-8273-f396841183c7",
                        "artifactId": "5cbdb181-e678-4a86-8651-d0007bf4f42f",
                        "rootFolder": "Tables",
                    },
                )

                sink = properties["sink"]
                sink_dataset = sink["datasetSettings"]
                self.assertEqual(sink["type"], "DataWarehouseSink")
                self.assertEqual(sink["tableOption"], "useExisting")
                self.assertEqual(sink["writeBehavior"], "Insert")
                self.assertEqual(
                    sink["preCopyScript"],
                    f"TRUNCATE TABLE dbo.stg_{table_name};",
                )
                self.assertEqual(sink_dataset["type"], "DataWarehouseTable")
                self.assertEqual(sink_dataset["typeProperties"]["schema"], "dbo")
                self.assertEqual(
                    sink_dataset["typeProperties"]["table"], f"stg_{table_name}"
                )
                self.assertEqual(
                    sink_dataset["linkedService"]["properties"]["typeProperties"][
                        "artifactId"
                    ],
                    "f80903f2-d5e7-4075-b270-304afb98fd2b",
                )

        self.assertEqual(
            [activity["name"] for activity in activities],
            ["BronzeIngestion", "SilverGoldTransformation", *copy_names],
        )
        self.assertFalse(any(activity["type"] == "Script" for activity in activities))


if __name__ == "__main__":
    unittest.main()
