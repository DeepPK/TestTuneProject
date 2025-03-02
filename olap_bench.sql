--olap_bench.sql
\set start_time random(1, 365)
SELECT 
    date_trunc('month', event_time) AS month,
    region,
    SUM(value) AS total_value,
    AVG(value) AS avg_value
FROM historical_data
WHERE event_time >= now() - (interval '1 day' * :start_time)
GROUP BY 1, 2
ORDER BY 1, 2;

-- 2. JOIN с таблицей категорий
\set category_id random(1, 1000)
SELECT 
    c.name,
    COUNT(h.id) AS event_count,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY h.value) AS median_value
FROM historical_data h
JOIN categories c ON h.category_id = c.category_id
WHERE c.category_id = :category_id
GROUP BY c.name;

-- 3. Оконные функции
SELECT 
    region,
    event_time,
    value,
    AVG(value) OVER(PARTITION BY region ORDER BY event_time ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg
FROM historical_data
WHERE event_time >= now() - interval '1 year';

-- 4. Гео-аналитика
\set region_count random(1, 10)
SELECT 
    region,
    CORR(EXTRACT(EPOCH FROM event_time), value) AS time_value_corr
FROM historical_data
GROUP BY region
LIMIT :region_count;