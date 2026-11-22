# Retail solution review findings

This review covers the synthetic CSV contract, Bronze and Silver/Gold
notebooks, pipeline configuration, and Warehouse scripts. No production
definitions were changed as part of this review.

## Observed issues and operational gaps

1. **Warehouse staging load is now defined in the repository pipeline.** Five
   Copy activities load the Gold tables into their Warehouse staging tables.
   The bindings were copied from the existing live pipeline definition; this
   repository change has not been synced to or run in the live workspace.
   Publishing and validation remain manual Warehouse SQL steps because a
   Script activity connection binding is not established.
2. **Live pipeline bindings are workspace-specific.** The repository pipeline
   uses the actual workspace, Lakehouse, Warehouse, and notebook runtime IDs
   observed in the connected workspace. Verify the existing item connections
   and default Lakehouse attachments remain available after deployment.
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

1. Validate the checked-in activity definitions and confirm that the recorded
   workspace and item bindings plus each notebook's default Lakehouse
   attachment resolve in Fabric before deployment.
2. Decide whether Bronze input is append/upsert data or a full snapshot.
   Implement and test deletion/soft-delete handling if source removals must
   propagate.
3. Write each Silver/Gold run to versioned staging tables, validate all tables,
   and promote the full set together (or provide rollback) to avoid mixed
   snapshots after failure.
4. Deploy `scripts/05_create_snapshot_staging.sql` before running the pipeline.
   After the five copies succeed, manually run the publish and validation SQL
   scripts in the Warehouse query editor. Configure and verify a supported
   Script activity connection before automating those steps.
5. Either implement date/environment routing or rename/document these
   parameters as informational context only.
6. Keep the validation SQL in deployment operations because Warehouse
   constraints do not enforce key uniqueness or referential integrity.

The remaining items above are recommendations and deployment risks, not
changes made to the live Fabric items or Warehouse object definitions.

## Validation status

The checked-in CSVs currently pass local row-count, key-uniqueness,
referential-integrity, and sales-amount checks (120 customers, 42 products,
8 stores, 2,500 sales lines). Python syntax, pipeline JSON, SQL object
structure, and the Warehouse SQL project build were checked locally. The
PySpark unit tests could not execute in this environment because PySpark and a
Java runtime are unavailable; they require a local Spark environment or a
Fabric notebook runtime.
