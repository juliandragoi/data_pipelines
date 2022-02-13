
DROP TABLE IF EXISTS core.news;

CREATE TABLE IF NOT EXISTS core.news (
    post_id serial
	, source varchar
	, title	varchar
	, summary varchar
	, link varchar
	, author varchar
	, published varchar
	, captured_at varchar
	, CONSTRAINT unique_news_constraint UNIQUE (source,title)
);

ALTER TABLE core.feeds OWNER TO pi_user;

DROP TABLE IF EXISTS core.feeds;

CREATE TABLE IF NOT EXISTS core.feeds (
    post_id serial
	, url varchar
	, status varchar
	, captured_at varchar
	, CONSTRAINT unique_feeds_constraint UNIQUE (url,status)
);

ALTER TABLE core.feeds OWNER TO pi_user;