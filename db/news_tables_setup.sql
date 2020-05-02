
DROP TABLE IF EXISTS core.news_raw;

CREATE TABLE IF NOT EXISTS core.news_raw (
	brand varchar
	, title	varchar
	, summary varchar
	, description varchar
	, "content" varchar
	, captured_at timestamp DEFAULT now()
)

ALTER TABLE core.news_raw ADD CONSTRAINT title_constraint UNIQUE (title);

ALTER TABLE core.news_raw OWNER TO pi_user;