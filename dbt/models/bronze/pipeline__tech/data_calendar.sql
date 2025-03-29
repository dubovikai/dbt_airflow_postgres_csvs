SELECT DATE(gs.dt) AS date
FROM generate_series('2024-03-25', '2024-03-28', INTERVAL '1 day') AS gs(dt)