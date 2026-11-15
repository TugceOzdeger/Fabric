# Retail solution review findings

This review covers the synthetic CSV contract, Bronze and Silver/Gold
notebooks, pipeline configuration, and Warehouse scripts. No production
definitions were changed as part of this review.

## Observed issues and operational gaps

1. **Warehouse loading is outside the pipeline.** The pipeline contains only
   `BronzeIngestion` followed by `SilverGoldTransformation`; it has no activity
   to copy the five Gold tables into `Bronze_Warehouse`. The Warehouse tables
   and views therefore remain empty until a separate copy/load step is run.
2. **The pipeline workspace binding needs deployment verification.** Both
   notebook activities set `workspaceId` to
   `00000000-0000-0000-0000-000000000000`. This may be a current-workspace
   sentinel in Fabric, but the repository alone does not prove that the target
   workspace will resolve it.
3. **Bronze upserts do not propagate deletions.** `Bronze_Notebook` merges
   input rows by business key and explicitly leaves target rows that are
   absent from a later source file. If source files represent full snapshots,
   deleted source records remain in Bronze and continue into Silver/Gold.
4. **Silver/Gold publication is not atomic.** The notebook overwrites Silver
   outputs, quarantine, dimensions, and fact tables sequentially. A failure
   partway through can leave tables from different run versions in the
   Lakehouse.
5. **Run parameters do not control processing.** `processing_date` and
   `environment` are validated/logged as run context, but neither parameter
   filters rows, chooses an environment, nor routes outputs.
6. **Warehouse constraints are metadata-only.** The PK, UNIQUE, and FK
   constraints use Fabric's `NOT ENFORCED` form; malformed or duplicate rows
   can still be loaded. The deployment scripts are manual query-editor scripts
   and are not part of the DACPAC model build.

## Recommended improvements

1. Confirm or replace the all-zero workspace binding during Fabric deployment,
   and validate notebook IDs plus each notebook's default Lakehouse reference.
2. Decide whether Bronze input is append/upsert data or a full snapshot.
   Implement and test deletion/soft-delete handling if source removals must
   propagate.
3. Write each Silver/Gold run to versioned staging tables, validate all tables,
   and promote the full set together (or provide rollback) to avoid mixed
   snapshots after failure.
4. Add a Warehouse load activity after successful Silver/Gold processing, with
   explicit snapshot replacement semantics and a final execution of
   `scripts/04_validate_model.sql`.
5. Either implement date/environment routing or rename/document these
   parameters as informational context only.
6. Keep the validation SQL in deployment operations because Warehouse
   constraints do not enforce key uniqueness or referential integrity.

The items above are recommendations and deployment risks, not changes made to
the production notebooks, pipeline definition, or Warehouse object
definitions in this review.

## Validation status

The checked-in CSVs currently pass local row-count, key-uniqueness,
referential-integrity, and sales-amount checks (120 customers, 42 products,
8 stores, 2,500 sales lines). Python syntax, pipeline JSON, SQL object
structure, and the Warehouse SQL project build were checked locally. The
PySpark unit tests could not execute in this environment because PySpark and a
Java runtime are unavailable; they require a local Spark environment or a
Fabric notebook runtime.
