SELECT 
    strftime('%Y', sale_date) AS year,
    SUM(amount) AS total_amount,
    COUNT(*) AS total_sales
FROM sales
GROUP BY year
ORDER BY year;
