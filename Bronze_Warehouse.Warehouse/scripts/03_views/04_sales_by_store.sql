CREATE OR ALTER VIEW dbo.vw_sales_by_store
AS
SELECT
    s.StoreKey,
    s.StoreID,
    s.StoreName,
    s.City,
    s.Region,
    s.StoreType,
    COALESCE(SUM(fs.LineTotal), CONVERT(decimal(38,2), 0)) AS RevenueUSD,
    COALESCE(SUM(CONVERT(bigint, fs.Quantity)), CONVERT(bigint, 0)) AS UnitsSold,
    COUNT_BIG(fs.FactSalesKey) AS SalesLineCount
FROM dbo.dim_store AS s
LEFT JOIN dbo.fact_sales AS fs
    ON fs.StoreKey = s.StoreKey
GROUP BY
    s.StoreKey,
    s.StoreID,
    s.StoreName,
    s.City,
    s.Region,
    s.StoreType;
