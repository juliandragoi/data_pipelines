DROP TABLE IF EXISTS core.trends;

CREATE TABLE core.trends(
	id  			bigint,
	keyword 		varchar,
	"source"		varchar,
	captured_at 	timestamp DEFAULT now(),
	CONSTRAINT tweet_constraint UNIQUE (id,keyword)
)