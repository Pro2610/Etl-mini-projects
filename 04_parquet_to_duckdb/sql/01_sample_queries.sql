-- Список валют і ключів
SELECT * FROM dim_currency ORDER BY symbol;

-- Кількість записів по даті
SELECT date, COUNT(*) AS n
FROM fact_rates
GROUP BY date
ORDER BY date DESC;

-- Приклад аналітики: середній курс у розрізі валют
SELECT d.symbol, AVG(f.rate) AS avg_rate
FROM fact_rates f
JOIN dim_currency d USING(currency_key)
GROUP BY d.symbol
ORDER BY d.symbol;
