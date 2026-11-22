-- Run only after all five Gold tables have been copied to their matching
-- staging tables. Validation happens before the existing Warehouse snapshot is
-- changed; all five replacements then commit or roll back together.
IF NOT EXISTS (SELECT 1 FROM dbo.stg_dim_customer)
BEGIN
    ;THROW 51000, 'stg_dim_customer is empty; refusing to publish.', 1;
END;
IF NOT EXISTS (SELECT 1 FROM dbo.stg_dim_product)
BEGIN
    ;THROW 51000, 'stg_dim_product is empty; refusing to publish.', 1;
END;
IF NOT EXISTS (SELECT 1 FROM dbo.stg_dim_store)
BEGIN
    ;THROW 51000, 'stg_dim_store is empty; refusing to publish.', 1;
END;
IF NOT EXISTS (SELECT 1 FROM dbo.stg_dim_date)
BEGIN
    ;THROW 51000, 'stg_dim_date is empty; refusing to publish.', 1;
END;
IF NOT EXISTS (SELECT 1 FROM dbo.stg_fact_sales)
BEGIN
    ;THROW 51000, 'stg_fact_sales is empty; refusing to publish.', 1;
END;

IF EXISTS
(
    SELECT CustomerKey FROM dbo.stg_dim_customer
    GROUP BY CustomerKey
    HAVING CustomerKey IS NULL OR COUNT_BIG(*) > 1
)
BEGIN
    ;THROW 51000, 'stg_dim_customer has null or duplicate CustomerKey values.', 1;
END;
IF EXISTS
(
    SELECT CustomerID FROM dbo.stg_dim_customer
    GROUP BY CustomerID
    HAVING CustomerID IS NULL OR COUNT_BIG(*) > 1
)
BEGIN
    ;THROW 51000, 'stg_dim_customer has null or duplicate CustomerID values.', 1;
END;

IF EXISTS
(
    SELECT ProductKey FROM dbo.stg_dim_product
    GROUP BY ProductKey
    HAVING ProductKey IS NULL OR COUNT_BIG(*) > 1
)
BEGIN
    ;THROW 51000, 'stg_dim_product has null or duplicate ProductKey values.', 1;
END;
IF EXISTS
(
    SELECT ProductID FROM dbo.stg_dim_product
    GROUP BY ProductID
    HAVING ProductID IS NULL OR COUNT_BIG(*) > 1
)
BEGIN
    ;THROW 51000, 'stg_dim_product has null or duplicate ProductID values.', 1;
END;

IF EXISTS
(
    SELECT StoreKey FROM dbo.stg_dim_store
    GROUP BY StoreKey
    HAVING StoreKey IS NULL OR COUNT_BIG(*) > 1
)
BEGIN
    ;THROW 51000, 'stg_dim_store has null or duplicate StoreKey values.', 1;
END;
IF EXISTS
(
    SELECT StoreID FROM dbo.stg_dim_store
    GROUP BY StoreID
    HAVING StoreID IS NULL OR COUNT_BIG(*) > 1
)
BEGIN
    ;THROW 51000, 'stg_dim_store has null or duplicate StoreID values.', 1;
END;

IF EXISTS
(
    SELECT DateKey FROM dbo.stg_dim_date
    GROUP BY DateKey
    HAVING DateKey IS NULL OR COUNT_BIG(*) > 1
)
BEGIN
    ;THROW 51000, 'stg_dim_date has null or duplicate DateKey values.', 1;
END;
IF EXISTS
(
    SELECT [Date] FROM dbo.stg_dim_date
    GROUP BY [Date]
    HAVING [Date] IS NULL OR COUNT_BIG(*) > 1
)
BEGIN
    ;THROW 51000, 'stg_dim_date has null or duplicate Date values.', 1;
END;
IF EXISTS
(
    SELECT 1
    FROM dbo.stg_dim_date
    WHERE DateKey <> CONVERT(int, CONVERT(char(8), [Date], 112))
       OR [Year] <> DATEPART(year, [Date])
       OR [Quarter] <> DATEPART(quarter, [Date])
       OR [Month] <> DATEPART(month, [Date])
       OR DayOfMonth <> DATEPART(day, [Date])
)
BEGIN
    ;THROW 51000, 'stg_dim_date contains inconsistent calendar attributes.', 1;
END;

IF EXISTS
(
    SELECT FactSalesKey FROM dbo.stg_fact_sales
    GROUP BY FactSalesKey
    HAVING FactSalesKey IS NULL OR COUNT_BIG(*) > 1
)
BEGIN
    ;THROW 51000, 'stg_fact_sales has null or duplicate FactSalesKey values.', 1;
END;
IF EXISTS
(
    SELECT TransactionID FROM dbo.stg_fact_sales
    GROUP BY TransactionID
    HAVING TransactionID IS NULL OR COUNT_BIG(*) > 1
)
BEGIN
    ;THROW 51000, 'stg_fact_sales has null or duplicate TransactionID values.', 1;
END;
IF EXISTS
(
    SELECT 1
    FROM dbo.stg_fact_sales AS f
    LEFT JOIN dbo.stg_dim_date AS d ON d.DateKey = f.DateKey
    LEFT JOIN dbo.stg_dim_customer AS c ON c.CustomerKey = f.CustomerKey
    LEFT JOIN dbo.stg_dim_product AS p ON p.ProductKey = f.ProductKey
    LEFT JOIN dbo.stg_dim_store AS s ON s.StoreKey = f.StoreKey
    WHERE d.DateKey IS NULL
       OR c.CustomerKey IS NULL
       OR p.ProductKey IS NULL
       OR s.StoreKey IS NULL
)
BEGIN
    ;THROW 51000, 'stg_fact_sales contains foreign keys absent from the staged dimensions.', 1;
END;
IF EXISTS
(
    SELECT 1
    FROM dbo.stg_fact_sales
    WHERE Quantity <= 0
       OR UnitPrice <= 0
       OR LineTotal <> Quantity * UnitPrice
)
BEGIN
    ;THROW 51000, 'stg_fact_sales contains invalid quantities, prices, or line totals.', 1;
END;

BEGIN TRY
    BEGIN TRANSACTION;

    DELETE FROM dbo.fact_sales;
    DELETE FROM dbo.dim_date;
    DELETE FROM dbo.dim_store;
    DELETE FROM dbo.dim_product;
    DELETE FROM dbo.dim_customer;

    INSERT INTO dbo.dim_customer
        (CustomerKey, CustomerID, CustomerSegment, City, Region, SignupDate)
    SELECT CustomerKey, CustomerID, CustomerSegment, City, Region, SignupDate
    FROM dbo.stg_dim_customer;

    INSERT INTO dbo.dim_product
        (ProductKey, ProductID, ProductName, Category, UnitPrice, UnitCost)
    SELECT ProductKey, ProductID, ProductName, Category, UnitPrice, UnitCost
    FROM dbo.stg_dim_product;

    INSERT INTO dbo.dim_store
        (StoreKey, StoreID, StoreName, City, Region, StoreType)
    SELECT StoreKey, StoreID, StoreName, City, Region, StoreType
    FROM dbo.stg_dim_store;

    INSERT INTO dbo.dim_date
        (DateKey, [Date], [Year], [Quarter], [Month], MonthName, DayOfMonth,
         DayOfWeek, DayName, IsWeekend)
    SELECT DateKey, [Date], [Year], [Quarter], [Month], MonthName, DayOfMonth,
           DayOfWeek, DayName, IsWeekend
    FROM dbo.stg_dim_date;

    INSERT INTO dbo.fact_sales
        (FactSalesKey, TransactionID, DateKey, CustomerKey, ProductKey, StoreKey,
         Quantity, UnitPrice, LineTotal)
    SELECT FactSalesKey, TransactionID, DateKey, CustomerKey, ProductKey, StoreKey,
           Quantity, UnitPrice, LineTotal
    FROM dbo.stg_fact_sales;

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;
    THROW;
END CATCH;
