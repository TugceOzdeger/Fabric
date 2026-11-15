CREATE OR ALTER VIEW dbo.vw_total_sales
AS
SELECT
    SUM(fs.LineTotal) AS TotalSalesUSD,
    SUM(CONVERT(bigint, fs.Quantity)) AS TotalUnitsSold,
    COUNT_BIG(*) AS SalesLineCount,
    COUNT_BIG(DISTINCT fs.TransactionID) AS TransactionCount
FROM dbo.fact_sales AS fs;
