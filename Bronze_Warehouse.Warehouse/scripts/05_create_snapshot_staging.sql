-- Create load-only tables for complete Gold snapshots. These tables intentionally
-- have no keys: validate the staged batch before publishing it.
IF OBJECT_ID(N'dbo.stg_dim_customer', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.stg_dim_customer
    (
        CustomerKey      varchar(64)  NOT NULL,
        CustomerID       varchar(20)  NOT NULL,
        CustomerSegment  varchar(40)  NOT NULL,
        City             varchar(100) NULL,
        Region           varchar(100) NULL,
        SignupDate       date         NOT NULL
    );
END;

IF OBJECT_ID(N'dbo.stg_dim_product', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.stg_dim_product
    (
        ProductKey  varchar(64)   NOT NULL,
        ProductID   varchar(20)   NOT NULL,
        ProductName varchar(200)  NOT NULL,
        Category    varchar(100)  NOT NULL,
        UnitPrice   decimal(12,2) NOT NULL,
        UnitCost    decimal(12,2) NOT NULL
    );
END;

IF OBJECT_ID(N'dbo.stg_dim_store', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.stg_dim_store
    (
        StoreKey  varchar(64)  NOT NULL,
        StoreID   varchar(20)  NOT NULL,
        StoreName varchar(200) NOT NULL,
        City      varchar(100) NOT NULL,
        Region    varchar(100) NOT NULL,
        StoreType varchar(40)  NOT NULL
    );
END;

IF OBJECT_ID(N'dbo.stg_dim_date', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.stg_dim_date
    (
        DateKey     int         NOT NULL,
        [Date]      date        NOT NULL,
        [Year]      int         NOT NULL,
        [Quarter]   int         NOT NULL,
        [Month]     int         NOT NULL,
        MonthName   varchar(9)  NOT NULL,
        DayOfMonth  int         NOT NULL,
        DayOfWeek   int         NOT NULL,
        DayName     varchar(9)  NOT NULL,
        IsWeekend   bit         NOT NULL
    );
END;

IF OBJECT_ID(N'dbo.stg_fact_sales', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.stg_fact_sales
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
