\set id random(1, 1000)
\set hits random(1, 1000000)
INSERT INTO counters (id, hits) VALUES (:id, :hits) ON CONFLICT (id) DO UPDATE SET hits = EXCLUDED.hits;
