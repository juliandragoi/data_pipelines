UPDATE pg_database SET datistemplate = FALSE WHERE datname = 'template1';
DROP DATABASE template1;
CREATE DATABASE template1 WITH TEMPLATE = template0 ENCODING 'UTF8' LC_CTYPE 'en_GB.UTF-8' LC_COLLATE 'en_GB.UTF-8';
CREATE DATABASE template1 WITH ENCODING 'UTF8' LC_CTYPE 'en_GB.UTF-8' LC_COLLATE 'en_GB.UTF-8';

DROP DATABASE IF EXISTS pi4_db;
CREATE DATABASE pi4_db;

update pg_database set encoding = pg_char_to_encoding('UTF8') where datname = 'pi4_db'

DROP SCHEMA IF EXISTS staging;
DROP SCHEMA IF EXISTS core;

CREATE SCHEMA staging;
CREATE SCHEMA core;

CREATE USER pi_user WITH PASSWORD '';

GRANT ALL PRIVILEGES ON DATABASE "postgres" to pi_user;
GRANT ALL PRIVILEGES ON DATABASE "pi4_db" to pi_user;

GRANT ALL PRIVILEGES ON SCHEMA "staging" to pi_user
GRANT ALL PRIVILEGES ON SCHEMA "core" to pi_user;