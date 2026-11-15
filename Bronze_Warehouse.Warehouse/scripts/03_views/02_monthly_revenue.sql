CREATE OR ALTER VIEW dbo.vw_monthly_revenue
AS
SELECT
    d.[Year],
    d.[Month],
    d.MonthName,
    d.[Year] * 100 + d.[Month] AS MonthKey,
    DATEFROMPARTS(d.[Year], d.[Month], 1) AS MonthStartDate,
    SUM(fs.LineTotal) AS RevenueUSD,
    SUM(CONVERT(bigint, fs.Quantity)) AS UnitsSold,
    COUNT_BIG(*) AS SalesLineCount,
    COUNT_BIG(DISTINCT fs.TransactionID) AS TransactionCount
FROM dbo.fact_sales AS fs
INNER JOIN dbo.dim_date AS d
    ON d.DateKey = fs.DateKey
GROUP BY
    d.[Year],
    d.[Month],
    d.MonthName;
