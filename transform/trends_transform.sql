INSERT INTO core.trends  (keyword, "source")
SELECT keywords, 'google_trends'FROM staging.google_trends_today
ON CONFLICT ON CONSTRAINT trends_constraint
DO NOTHING;


INSERT INTO core.trends  (keyword, "source")
SELECT keywords, 'twitter_trends'FROM staging.twitter_trends_today_uk
ON CONFLICT ON CONSTRAINT trends_constraint
DO NOTHING;
