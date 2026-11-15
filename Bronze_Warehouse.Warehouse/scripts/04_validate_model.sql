-- Each query returns violating rows. Empty results indicate that check passed.

-- 1. Null or duplicate surrogate and source keys.
SELECT
    'dim_customer.CustomerKey' AS KeyName,
    CONVERT(varchar(64), CustomerKey) AS KeyValue,
    COUNT_BIG(*) AS RowCount
FROM dbo.dim_customer
GROUP BY CustomerKey
HAVING CustomerKey IS NULL OR COUNT_BIG(*) > 1
UNION ALL
SELECT
    'dim_customer.CustomerID',
    CONVERT(varchar(64), CustomerID),
    COUNT_BIG(*)
FROM dbo.dim_customer
GROUP BY CustomerID
HAVING CustomerID IS NULL OR COUNT_BIG(*) > 1
UNION ALL
SELECT
    'dim_product.ProductKey',
    CONVERT(varchar(64), ProductKey),
    COUNT_BIG(*)
FROM dbo.dim_product
GROUP BY ProductKey
HAVING ProductKey IS NULL OR COUNT_BIG(*) > 1
UNION ALL
SELECT
    'dim_product.ProductID',
    CONVERT(varchar(64), ProductID),
    COUNT_BIG(*)
FROM dbo.dim_product
GROUP BY ProductID
HAVING ProductID IS NULL OR COUNT_BIG(*) > 1
UNION ALL
SELECT
    'dim_store.StoreKey',
    CONVERT(varchar(64), StoreKey),
    COUNT_BIG(*)
FROM dbo.dim_store
GROUP BY StoreKey
HAVING StoreKey IS NULL OR COUNT_BIG(*) > 1
UNION ALL
SELECT
    'dim_store.StoreID',
    CONVERT(varchar(64), StoreID),
    COUNT_BIG(*)
FROM dbo.dim_store
GROUP BY StoreID
HAVING StoreID IS NULL OR COUNT_BIG(*) > 1
UNION ALL
SELECT
    'dim_date.DateKey',
    CONVERT(varchar(64), DateKey),
    COUNT_BIG(*)
FROM dbo.dim_date
GROUP BY DateKey
HAVING DateKey IS NULL OR COUNT_BIG(*) > 1
UNION ALL
SELECT
    'dim_date.Date',
    CONVERT(varchar(64), [Date], 23),
    COUNT_BIG(*)
FROM dbo.dim_date
GROUP BY [Date]
HAVING [Date] IS NULL OR COUNT_BIG(*) > 1
UNION ALL
SELECT
    'fact_sales.FactSalesKey',
    CONVERT(varchar(64), FactSalesKey),
    COUNT_BIG(*)
FROM dbo.fact_sales
GROUP BY FactSalesKey
HAVING FactSalesKey IS NULL OR COUNT_BIG(*) > 1
UNION ALL
SELECT
    'fact_sales.TransactionID',
    CONVERT(varchar(64), TransactionID),
    COUNT_BIG(*)
FROM dbo.fact_sales
GROUP BY TransactionID
HAVING TransactionID IS NULL OR COUNT_BIG(*) > 1;

-- 2. Fact rows without a matching dimension row.
SELECT
    f.FactSalesKey,
    'DateKey' AS RelationshipName,
    CONVERT(varchar(64), f.DateKey) AS MissingKey
FROM dbo.fact_sales AS f
LEFT JOIN dbo.dim_date AS d
    ON d.DateKey = f.DateKey
WHERE d.DateKey IS NULL
UNION ALL
SELECT
    f.FactSalesKey,
    'CustomerKey',
    f.CustomerKey
FROM dbo.fact_sales AS f
LEFT JOIN dbo.dim_customer AS d
    ON d.CustomerKey = f.CustomerKey
WHERE d.CustomerKey IS NULL
UNION ALL
SELECT
    f.FactSalesKey,
    'ProductKey',
    f.ProductKey
FROM dbo.fact_sales AS f
LEFT JOIN dbo.dim_product AS d
    ON d.ProductKey = f.ProductKey
WHERE d.ProductKey IS NULL
UNION ALL
SELECT
    f.FactSalesKey,
    'StoreKey',
    f.StoreKey
FROM dbo.fact_sales AS f
LEFT JOIN dbo.dim_store AS d
    ON d.StoreKey = f.StoreKey
WHERE d.StoreKey IS NULL;

-- 3. Sales lines with invalid quantities, prices, or extended amounts.
SELECT
    FactSalesKey,
    TransactionID,
    Quantity,
    UnitPrice,
    LineTotal
FROM dbo.fact_sales
WHERE Quantity <= 0
   OR UnitPrice <= 0
   OR LineTotal <> Quantity * UnitPrice;

-- 4. Date keys and core calendar attributes must agree with the date.
SELECT
    DateKey,
    [Date],
    [Year],
    [Quarter],
    [Month],
    DayOfMonth
FROM dbo.dim_date
WHERE DateKey <> CONVERT(int, CONVERT(char(8), [Date], 112))
   OR [Year] <> DATEPART(year, [Date])
   OR [Quarter] <> DATEPART(quarter, [Date])
   OR [Month] <> DATEPART(month, [Date])
   OR DayOfMonth <> DATEPART(day, [Date]);
