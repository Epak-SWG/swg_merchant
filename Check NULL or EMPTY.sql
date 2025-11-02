SELECT * FROM sales
WHERE amount IS NULL OR amount < 0
   OR vendor = '' OR item = ''
   OR sale_date IS NULL OR sale_date = '';
