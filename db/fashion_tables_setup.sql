

DROP TABLE IF EXISTS core.fashion_tweets;

CREATE TABLE IF NOT EXISTS core.fashion_tweets (
	created_at 		timestamp
	, id			bigint
	, screen_name 	varchar
	, "name" 		varchar
	, user_desc 	varchar
	, tweet_text 	varchar
	, "location" 	varchar
	, captured_at 	timestamp DEFAULT now()
	, CONSTRAINT tweet_constraint UNIQUE (id,tweet_text)
)

ALTER TABLE core.fashion_tweets OWNER TO pi_user;