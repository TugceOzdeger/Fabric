-- Create the Gold-aligned sales fact at one row per sales transaction line.
IF OBJECT_ID(N'dbo.fact_sales', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.fact_sales
    (
        FactSalesKey varchar(64)   NOT NULL,
        TransactionID varchar(20)  NOT NULL,
        DateKey       int          NOT NULL,
        CustomerKey   varchar(64)  NOT NULL,
        ProductKey    varchar(64)  NOT NULL,
        StoreKey      varchar(64)  NOT NULL,
        Quantity      int          NOT NULL,
        UnitPrice     decimal(12,2) NOT NULL,
        LineTotal     decimal(12,2) NOT NULL
    );
END;

IF NOT EXISTS
       (SELECT 1 FROM sys.key_constraints
        WHERE name = N'PK_fact_sales'
          AND parent_object_id = OBJECT_ID(N'dbo.fact_sales'))
BEGIN
    ALTER TABLE dbo.fact_sales
        ADD CONSTRAINT PK_fact_sales
        PRIMARY KEY NONCLUSTERED (FactSalesKey) NOT ENFORCED;
END;

-- TransactionID is the source business key and is unique in the sample model.
IF NOT EXISTS
       (SELECT 1 FROM sys.key_constraints
        WHERE name = N'UQ_fact_sales_TransactionID'
          AND parent_object_id = OBJECT_ID(N'dbo.fact_sales'))
BEGIN
    ALTER TABLE dbo.fact_sales
        ADD CONSTRAINT UQ_fact_sales_TransactionID
        UNIQUE NONCLUSTERED (TransactionID) NOT ENFORCED;
END;

IF NOT EXISTS
       (SELECT 1 FROM sys.foreign_keys
        WHERE name = N'FK_fact_sales_dim_date'
          AND parent_object_id = OBJECT_ID(N'dbo.fact_sales'))
BEGIN
    ALTER TABLE dbo.fact_sales
        ADD CONSTRAINT FK_fact_sales_dim_date
        FOREIGN KEY (DateKey) REFERENCES dbo.dim_date (DateKey) NOT ENFORCED;
END;

IF NOT EXISTS
       (SELECT 1 FROM sys.foreign_keys
        WHERE name = N'FK_fact_sales_dim_customer'
          AND parent_object_id = OBJECT_ID(N'dbo.fact_sales'))
BEGIN
    ALTER TABLE dbo.fact_sales
        ADD CONSTRAINT FK_fact_sales_dim_customer
        FOREIGN KEY (CustomerKey) REFERENCES dbo.dim_customer (CustomerKey) NOT ENFORCED;
END;

IF NOT EXISTS
       (SELECT 1 FROM sys.foreign_keys
        WHERE name = N'FK_fact_sales_dim_product'
          AND parent_object_id = OBJECT_ID(N'dbo.fact_sales'))
BEGIN
    ALTER TABLE dbo.fact_sales
        ADD CONSTRAINT FK_fact_sales_dim_product
        FOREIGN KEY (ProductKey) REFERENCES dbo.dim_product (ProductKey) NOT ENFORCED;
END;

IF NOT EXISTS
       (SELECT 1 FROM sys.foreign_keys
        WHERE name = N'FK_fact_sales_dim_store'
          AND parent_object_id = OBJECT_ID(N'dbo.fact_sales'))
BEGIN
    ALTER TABLE dbo.fact_sales
        ADD CONSTRAINT FK_fact_sales_dim_store
        FOREIGN KEY (StoreKey) REFERENCES dbo.dim_store (StoreKey) NOT ENFORCED;
END;
