--data_gen_olap.sql
INSERT INTO historical_data (event_time, category_id, value, region, status)
VALUES (
    now() - (random() * interval '365 days'),
    random() * 1000,
    random() * 10000,
    (array['North','South','East','West'])[(random()*4)::int + 1],
    (random() * 3)::int
);

INSERT INTO categories (category_id, name)
SELECT g, md5(g::text)
FROM generate_series(1,1000) g
ON CONFLICT DO NOTHING;