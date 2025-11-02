SELECT COALESCE(category, 'Uncategorized') AS category, SUM(amount) AS revenue, COUNT(*) AS orders
FROM sales
GROUP BY category
ORDER BY revenue DESC;