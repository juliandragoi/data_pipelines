INSERT INTO core.news  (source, title, summary, link, author, published, captured_at)
SELECT source, title, summary, link, author, published, captured_at FROM staging.news_rss
ON CONFLICT ON CONSTRAINT unique_news_constraint
DO NOTHING;
