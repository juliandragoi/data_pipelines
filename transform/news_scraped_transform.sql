INSERT INTO core.news_scraped  (link, text, captured_at)
SELECT link, text, captured_at FROM staging.news_rss_scraped
ON CONFLICT ON CONSTRAINT unique_news_scraped_constraint
DO NOTHING;
