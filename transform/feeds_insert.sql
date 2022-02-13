INSERT INTO core.feeds  (url, status, captured_at)
SELECT url, status, captured_at FROM staging.feed_status
ON CONFLICT ON CONSTRAINT unique_feeds_constraint
DO NOTHING;