-- Create the Gold-aligned retail dimensions. Existing tables are left untouched.
IF OBJECT_ID(N'dbo.dim_customer', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.dim_customer
    (
        CustomerKey      varchar(64)  NOT NULL,
        CustomerID       varchar(20)  NOT NULL,
        CustomerSegment  varchar(40)  NOT NULL,
        City             varchar(100) NULL,
        Region           varchar(100) NULL,
        SignupDate       date         NOT NULL
    );
END;

IF OBJECT_ID(N'dbo.dim_product', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.dim_product
    (
        ProductKey  varchar(64)   NOT NULL,
        ProductID   varchar(20)   NOT NULL,
        ProductName varchar(200)  NOT NULL,
        Category    varchar(100)  NOT NULL,
        UnitPrice   decimal(12,2) NOT NULL,
        UnitCost    decimal(12,2) NOT NULL
    );
END;

IF OBJECT_ID(N'dbo.dim_store', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.dim_store
    (
        StoreKey  varchar(64)  NOT NULL,
        StoreID   varchar(20)  NOT NULL,
        StoreName varchar(200) NOT NULL,
        City      varchar(100) NOT NULL,
        Region    varchar(100) NOT NULL,
        StoreType varchar(40)  NOT NULL
    );
END;

IF OBJECT_ID(N'dbo.dim_date', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.dim_date
    (
        DateKey     int         NOT NULL,
        [Date]      date        NOT NULL,
        [Year]      int         NOT NULL,
        [Quarter]   int         NOT NULL,
        [Month]     int         NOT NULL,
        MonthName   varchar(9)  NOT NULL,
        DayOfMonth int         NOT NULL,
        DayOfWeek  int         NOT NULL,
        DayName    varchar(9)  NOT NULL,
        IsWeekend  bit         NOT NULL
    );
END;

-- Fabric Warehouse requires PK/UNIQUE constraints to be added with ALTER TABLE.
IF NOT EXISTS
       (SELECT 1 FROM sys.key_constraints
        WHERE name = N'PK_dim_customer'
          AND parent_object_id = OBJECT_ID(N'dbo.dim_customer'))
BEGIN
    ALTER TABLE dbo.dim_customer
        ADD CONSTRAINT PK_dim_customer
        PRIMARY KEY NONCLUSTERED (CustomerKey) NOT ENFORCED;
END;

IF NOT EXISTS
       (SELECT 1 FROM sys.key_constraints
        WHERE name = N'PK_dim_product'
          AND parent_object_id = OBJECT_ID(N'dbo.dim_product'))
BEGIN
    ALTER TABLE dbo.dim_product
        ADD CONSTRAINT PK_dim_product
        PRIMARY KEY NONCLUSTERED (ProductKey) NOT ENFORCED;
END;

IF NOT EXISTS
       (SELECT 1 FROM sys.key_constraints
        WHERE name = N'PK_dim_store'
          AND parent_object_id = OBJECT_ID(N'dbo.dim_store'))
BEGIN
    ALTER TABLE dbo.dim_store
        ADD CONSTRAINT PK_dim_store
        PRIMARY KEY NONCLUSTERED (StoreKey) NOT ENFORCED;
END;

IF NOT EXISTS
       (SELECT 1 FROM sys.key_constraints
        WHERE name = N'PK_dim_date'
          AND parent_object_id = OBJECT_ID(N'dbo.dim_date'))
BEGIN
    ALTER TABLE dbo.dim_date
        ADD CONSTRAINT PK_dim_date
        PRIMARY KEY NONCLUSTERED (DateKey) NOT ENFORCED;
END;
