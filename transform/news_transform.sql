INSERT INTO core.news_raw  (brand, title, link, summary)
SELECT brand, title, link, summary FROM staging.rrs_news
ON CONFLICT ON CONSTRAINT title_constraint
DO NOTHING;

INSERT INTO core.news_raw  (brand, title, description, link, "content")
SELECT brand, title, description, link, "content" FROM staging.api_news
ON CONFLICT ON CONSTRAINT title_constraint
DO NOTHING;
