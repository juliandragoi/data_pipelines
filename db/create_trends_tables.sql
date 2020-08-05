DROP TABLE IF EXISTS core.trends;

CREATE TABLE core.trends(
	id  			serial,
	keyword 		varchar,
	"source"		varchar,
	captured_at 	timestamp DEFAULT now(),
	CONSTRAINT trends_constraint UNIQUE (id,keyword)
)


DROP TABLE IF EXISTS core.trends;

CREATE TABLE core.trends(
	id  			serial,
	keyword 		varchar,
	"source"		varchar,
	captured_at 	timestamp DEFAULT now(),
	CONSTRAINT trends_constraint UNIQUE (id,keyword)
)




--twitter_trends_today_uk