# Synthetic Retail Sales Sample

This folder contains a fabricated retail dataset for Microsoft Fabric data-engineering examples. All records and locations are synthetic; no external data sources or real personal contact details are used.

## Files and row grain

| File | Rows | Grain |
| --- | ---: | --- |
| `customers.csv` | 120 | One row per customer |
| `products.csv` | 42 | One row per product |
| `stores.csv` | 8 | One row per store |
| `sales-transactions.csv` | 2,500 | One row per product line sold in a transaction |

All files are UTF-8 CSV with a header row. Dates use ISO `YYYY-MM-DD` format, identifiers are strings, quantities are whole numbers, and monetary values are decimal numbers with two places. Currency is synthetic **USD**.

## Columns and keys

- **customers.csv** — `CustomerID` (primary key), `CustomerSegment`, `City`, `Region`, `SignupDate`.
- **products.csv** — `ProductID` (primary key), `ProductName`, `Category`, `UnitPrice`, `UnitCost`.
- **stores.csv** — `StoreID` (primary key), `StoreName`, `City`, `Region`, `StoreType`.
- **sales-transactions.csv** — `TransactionID` (primary key), `TransactionDate`, `CustomerID`, `ProductID`, `StoreID`, `Quantity`, `UnitPrice`, `LineTotal`.

The sales table's `CustomerID`, `ProductID`, and `StoreID` are foreign keys to the corresponding dimension files. Every transaction references exactly one customer, product, and store.

## Dates and calculations

Sales transaction dates range from **2024-01-01 through 2025-12-31**. Customer signup dates are earlier than this range.

`UnitPrice` in each sales row is the product's listed `UnitPrice` at generation time. `LineTotal` is the pre-tax extended amount, calculated as `Quantity × UnitPrice`, rounded to two decimal places. This sample has no discounts, tax, shipping, or returns; sum `LineTotal` for gross sales revenue.
