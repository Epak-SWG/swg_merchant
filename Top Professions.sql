SELECT COALESCE(profession, 'Uncategorized') AS profession, SUM(amount) AS revenue, COUNT(*) AS orders
FROM sales
GROUP BY profession
ORDER BY revenue DESC;