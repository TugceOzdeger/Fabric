# Retail warehouse model

These deployment scripts add a dimensional sales model to the existing
`Bronze_Warehouse` Fabric Warehouse. They use the `dbo` schema and the same
dimension and fact names and SHA-256 surrogate-key columns produced by
`Silver_Gold_Notebook`.

## Deploy

1. In the Fabric workspace, open **Bronze_Warehouse** and its SQL query editor.
   Confirm that the connection is to the Warehouse, not the Lakehouse SQL
   analytics endpoint.
2. Run `scripts/01_dimensions.sql`, then `scripts/02_fact_sales.sql`.
   The fact script expects the four dimensions to exist.
3. Run each SQL file in `scripts/03_views/`. Each view file is a separate
   `CREATE OR ALTER VIEW` batch and can be rerun to update that view.
4. Copy the CSVs in `data/retail-sales/` to the attached Lakehouse's
   `Files/retail-sales/` folder, then run `Bronze_Notebook` and
   `Silver_Gold_Notebook` to ingest and transform the sample data.
5. Run `scripts/05_create_snapshot_staging.sql`.
6. Run `Bronze_Pipeline`. The checked-in pipeline reads `dim_customer`,
   `dim_product`, `dim_store`, `dim_date`, and `fact_sales` from the attached
   Lakehouse, then copies each into its matching `dbo.stg_*` table. Each
   Warehouse Copy activity runs `TRUNCATE TABLE dbo.stg_<table>;` before its
   load; the target dimensional tables are not truncated by the Copy
   activities. The five copies run only after successful Silver/Gold
   transformation. Serialize pipeline runs while the shared staging tables
   are in use.
7. After all five copies succeed, run `scripts/06_publish_snapshot.sql` in
   the Warehouse SQL query editor. It rejects empty or invalid staging
   snapshots, then replaces all five Warehouse tables in one Warehouse
   transaction. A failure before commit rolls back the target replacement.
   Run `scripts/04_validate_model.sql` afterward; inspect its result sets and
   resolve any returned rows before querying the views. The Warehouse
   constraints below are metadata only; they do not reject invalid or
   duplicate rows.

The repository pipeline uses the workspace, Lakehouse, Warehouse, and notebook
runtime IDs observed in the existing live pipeline. Its Copy activity
definitions use managed Lakehouse tables and existing Warehouse staging
tables. The `Bronze_Warehouse` linked service and its organizational-account
authentication must remain available in the target workspace. The existing
pipeline definition does not establish a connection binding for a Script
activity, so publishing and validation SQL remain manual until a supported
Fabric Warehouse Script connection is configured and verified.
See Microsoft's
[Lakehouse Copy activity configuration](https://learn.microsoft.com/en-us/fabric/data-factory/connector-lakehouse-copy-activity)
and [Warehouse Copy activity configuration](https://learn.microsoft.com/en-us/fabric/data-factory/connector-data-warehouse-copy-activity),
plus the [Script activity](https://learn.microsoft.com/en-us/fabric/data-factory/script-activity)
documentation.

The scripts create missing tables and constraints without dropping or
rebuilding existing objects. They do not migrate an existing table whose
columns differ from this model. Review and migrate any conflicting objects
before deployment. The warehouse was not connected to or modified while
preparing these scripts.

The staging and publish scripts are standalone Warehouse deployment scripts,
like the model scripts; they are excluded from the SQL project's DACPAC model
build. The publish script uses a single explicit transaction so readers see
either the previous or the new five-table Gold snapshot, not an intermediate
mix of old and new dimension/fact data.

## Model and grain

`fact_sales` has one row per source sales transaction line. Its measures are
quantity, the sale-time unit price, and `LineTotal` (pre-tax gross revenue).
The sample currency is USD and `LineTotal = Quantity * UnitPrice`. Dimensions
are Type 1 snapshots, matching the Silver/Gold notebook; the Gold date
dimension covers the minimum-to-maximum transaction dates in the loaded
snapshot.

All keys are nonclustered `NOT ENFORCED` constraints, as required by Fabric
Warehouse. Foreign keys are also `NOT ENFORCED`. The existing Silver/Gold
notebook validates business keys and fact references before producing its Gold
tables; keep equivalent validation in any other loader.

These files are standalone deployment scripts and are excluded from the SQL
project's DACPAC model build. Apply them through the Warehouse SQL query editor:
Fabric Git integration has limitations for adding Warehouse constraints.

## Data dictionary

### Tables

| Object | Grain | Columns |
| --- | --- | --- |
| `dbo.dim_customer` | One current row per customer | `CustomerKey` (`varchar(64)`, SHA-256 surrogate PK); `CustomerID` (`varchar(20)`, source key); `CustomerSegment` (`varchar(40)`); `City`, `Region` (`varchar(100)`, nullable); `SignupDate` (`date`). |
| `dbo.dim_product` | One current row per product | `ProductKey` (`varchar(64)`, SHA-256 surrogate PK); `ProductID` (`varchar(20)`, source key); `ProductName` (`varchar(200)`); `Category` (`varchar(100)`); `UnitPrice`, `UnitCost` (`decimal(12,2)`). |
| `dbo.dim_store` | One current row per store | `StoreKey` (`varchar(64)`, SHA-256 surrogate PK); `StoreID` (`varchar(20)`, source key); `StoreName` (`varchar(200)`); `City`, `Region` (`varchar(100)`); `StoreType` (`varchar(40)`). |
| `dbo.dim_date` | One row per calendar date in the sales range | `DateKey` (`int`, `yyyyMMdd` PK); `Date` (`date`); `Year`, `Quarter`, `Month`, `DayOfMonth`, `DayOfWeek` (`int`); `MonthName`, `DayName` (`varchar(9)`); `IsWeekend` (`bit`). `DayOfWeek` follows Spark's numbering: Sunday = 1 through Saturday = 7. |
| `dbo.fact_sales` | One row per transaction line | `FactSalesKey` (`varchar(64)`, SHA-256 PK); `TransactionID` (`varchar(20)`, unique source key); `DateKey`, `CustomerKey`, `ProductKey`, `StoreKey` (FKs); `Quantity` (`int`); `UnitPrice`, `LineTotal` (`decimal(12,2)`). |

Dimension hash keys are the lowercase 64-character SHA-256 hashes generated by
the notebook (`customer|<CustomerID>`, `product|<ProductID>`,
`store|<StoreID>`); `FactSalesKey` uses `sale|<TransactionID>`. Dates use
`yyyyMMdd` integer keys, as in the Gold `dim_date`.

### Views

| View | Purpose and principal measures |
| --- | --- |
| `dbo.vw_total_sales` | Single-row overall sales totals: USD revenue, units, sales lines, and distinct transactions. |
| `dbo.vw_monthly_revenue` | Revenue, units, sales lines, and distinct transactions by calendar month. |
| `dbo.vw_sales_by_product` | Revenue, units, and sales lines by product; includes products with no sales. |
| `dbo.vw_sales_by_store` | Revenue, units, and sales lines by store; includes stores with no sales. |
| `dbo.vw_customer_performance` | Revenue, units, sales lines, and distinct transactions by customer; includes customers with no sales. |

All revenue measures are USD, pre-tax, and based on `LineTotal`.
