SELECT c.name,
       SUM(s.amount)                          AS lifetime_spent,
       COUNT(*)                               AS purchases,
       MIN(date(s.sale_date))                 AS first_purchase,
       MAX(date(s.sale_date))                 AS last_purchase,
       ROUND(AVG(s.amount), 2)                AS avg_ticket
FROM customers c
JOIN sales s ON s.customer_id = c.id
GROUP BY c.id
ORDER BY lifetime_spent DESC;
