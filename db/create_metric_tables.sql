
DROP TABLE IF EXISTS core.table_metrics;

CREATE TABLE core.table_metrics(
	"schema"		varchar,
	table_name		varchar,
	row_count		int,
	size_mb 		varchar,
	captured_at 	timestamp DEFAULT now()
);

