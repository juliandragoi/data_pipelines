INSERT INTO core.fashion_tweets  (created_at, id, screen_name, "name", user_desc, tweet_text, "location" )
SELECT created_at, id, screen_name, "name", user_desc, tweet_text, "location"  FROM staging.fashion_brand_tweets
ON CONFLICT ON CONSTRAINT tweet_constraint
DO NOTHING;
