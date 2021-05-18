
DROP TABLE IF EXISTS core.news_scraped;

CREATE TABLE IF NOT EXISTS core.news_scraped(
    post_id serial
	, link varchar
	, text	varchar
	, captured_at varchar
	, CONSTRAINT unique_news_scraped_constraint UNIQUE (link,text)
)

ALTER TABLE core.news_scraped OWNER TO pi_user;