INSERT INTO core.news_raw  (brand, title, summary)
SELECT brand, title, summary FROM staging.rrs_news
ON CONFLICT ON CONSTRAINT title_constraint
DO NOTHING;

INSERT INTO core.news_raw  (brand, title, description, "content")
SELECT brand, title, description, "content" FROM staging.api_news
ON CONFLICT ON CONSTRAINT title_constraint
DO NOTHING;
