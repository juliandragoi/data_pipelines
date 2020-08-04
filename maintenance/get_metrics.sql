create or replace function
count_rows(schema text, tablename text) returns integer
as
$body$
declare
  result integer;
  query varchar;
begin
  query := 'SELECT count(1) FROM ' || schema || '.' || tablename;
  execute query into result;
  return result;
end;
$body$
language plpgsql;


INSERT INTO core.table_metrics ("schema", table_name, row_count, size_mb)
SELECT tbls.table_schema, tbls.table_name, tbls.rows_cnt,  pg_size_pretty(pg_total_relation_size(concat(tbls.table_schema,'.',tbls.table_name)))
FROM(select
  table_schema,
  table_name,
  count_rows(table_schema, table_name) AS rows_cnt
from information_schema.tables
where
  table_schema IN ('staging','core')) tbls;
