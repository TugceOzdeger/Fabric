CREATE OR ALTER VIEW dbo.vw_sales_by_product
AS
SELECT
    p.ProductKey,
    p.ProductID,
    p.ProductName,
    p.Category,
    COALESCE(SUM(fs.LineTotal), CONVERT(decimal(38,2), 0)) AS RevenueUSD,
    COALESCE(SUM(CONVERT(bigint, fs.Quantity)), CONVERT(bigint, 0)) AS UnitsSold,
    COUNT_BIG(fs.FactSalesKey) AS SalesLineCount
FROM dbo.dim_product AS p
LEFT JOIN dbo.fact_sales AS fs
    ON fs.ProductKey = p.ProductKey
GROUP BY
    p.ProductKey,
    p.ProductID,
    p.ProductName,
    p.Category;
