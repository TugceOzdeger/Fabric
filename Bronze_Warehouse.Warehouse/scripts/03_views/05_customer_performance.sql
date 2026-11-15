CREATE OR ALTER VIEW dbo.vw_customer_performance
AS
SELECT
    c.CustomerKey,
    c.CustomerID,
    c.CustomerSegment,
    c.City,
    c.Region,
    c.SignupDate,
    COALESCE(SUM(fs.LineTotal), CONVERT(decimal(38,2), 0)) AS TotalSalesUSD,
    COALESCE(SUM(CONVERT(bigint, fs.Quantity)), CONVERT(bigint, 0)) AS UnitsPurchased,
    COUNT_BIG(fs.FactSalesKey) AS SalesLineCount,
    COUNT_BIG(DISTINCT fs.TransactionID) AS TransactionCount
FROM dbo.dim_customer AS c
LEFT JOIN dbo.fact_sales AS fs
    ON fs.CustomerKey = c.CustomerKey
GROUP BY
    c.CustomerKey,
    c.CustomerID,
    c.CustomerSegment,
    c.City,
    c.Region,
    c.SignupDate;
