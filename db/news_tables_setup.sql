
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
)

ALTER TABLE core.news OWNER TO pi_user;