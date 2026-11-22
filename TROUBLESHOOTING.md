# Troubleshooting

## Notebook cannot find the CSV files

The Bronze notebook reads `Files/retail-sales/<file>.csv` from its default
Lakehouse. Upload all four repository CSVs to that folder and attach
`Bronze_Lakehouse` as the notebook's default Lakehouse. A local Git path is not
visible to the Fabric Spark runtime.

## Notebook reports a missing table or column

Run `Bronze_Notebook` successfully before `Silver_Gold_Notebook`. The latter
expects Bronze tables named `customers`, `products`, `stores`, and
`sales_transactions`, each with `ingestion_timestamp` and `source_file`
metadata columns. Confirm both notebooks point to the same Lakehouse.

## Rows are rejected or the Silver/Gold step fails validation

Check `silver_rejected_records` and the notebook logs for required values,
invalid dates, unsupported customer segments, nonpositive prices/quantities,
duplicate business keys, or sales that reference unknown customers, products,
or stores. Correct the source rows and rerun the pipeline. The Silver/Gold
notebook replaces its outputs from the current Bronze tables on every run.

## Removed source rows remain in Bronze

Bronze ingestion performs an upsert by business key and does not delete target
rows absent from a later CSV. If a CSV is a complete snapshot and deletions
should propagate, define an explicit delete/soft-delete policy or replace the
Bronze snapshot; rerunning the current upsert alone will retain stale rows.

## Pipeline activity cannot resolve a notebook or workspace

After deployment, verify the notebook IDs and workspace binding in
`Bronze_Pipeline.DataPipeline/pipeline-content.json`. The repository definition
uses the runtime workspace and notebook IDs from the existing pipeline
definition. Reattach the default Lakehouse after item deployment if needed.

## Warehouse views return no sales or dimensions

Creating the Warehouse schema and views does not copy Lakehouse data. The
checked-in pipeline copies all five Gold tables into their matching
`dbo.stg_*` tables, truncating only the staging table before its copy. It
does not publish the snapshot. After all five copies succeed, run
`scripts/06_publish_snapshot.sql` manually in the Warehouse SQL query editor,
then run `scripts/04_validate_model.sql`. The publish script preserves the
Gold `CustomerKey`, `ProductKey`, `StoreKey`, `DateKey`, and `FactSalesKey`
values. Do not overlap pipeline runs while sharing the staging tables. If a
copy cannot resolve a Lakehouse or Warehouse, check that the existing item
bindings and `Bronze_Warehouse` linked service remain available in the
workspace. `.platform` logical IDs are Git identities, not runtime item IDs.

## Snapshot publish refuses to run

The publish script intentionally stops when a staging table is empty, contains
duplicate keys, has invalid sales measures, has inconsistent date attributes,
or has sales referencing missing staged dimensions. Correct the source or
mapping, truncate and reload all five staging tables, and retry publication.
The production Warehouse tables are changed only after every staging check
passes, and the five-table replacement is transactional.

## Warehouse validation returns orphan or duplicate rows

Run `Bronze_Warehouse.Warehouse/scripts/04_validate_model.sql` and inspect the
returned check and key. Load dimensions before the fact, preserve the Gold hash
keys, and validate all four fact foreign keys. Fabric Warehouse constraints
are `NOT ENFORCED` and will not reject invalid rows for you.

## Local PySpark tests fail before running

Install the requirements in `requirements-test.txt` and a compatible Java
runtime, then rerun:

```sh
python -m pip install -r requirements-test.txt
python -m unittest discover -s tests -v
```

The tests load notebook function definitions without executing notebook
entrypoints, so they do not require Fabric or Delta table services. A local
Spark session still requires Java.

## SQL project build succeeds but does not deploy the Warehouse model

The scripts under `Bronze_Warehouse.Warehouse/scripts/` are standalone
deployment/query files and are excluded from the DACPAC model build. Apply
them through the Fabric Warehouse SQL query editor as described in the
Warehouse README. The build alone does not create Fabric tables or load data.
