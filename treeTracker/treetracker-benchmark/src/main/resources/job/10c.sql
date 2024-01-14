-- implement the select predicates as views on top of the tables for JOB 10c
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/10c.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q10c_cast_info CASCADE;
DROP VIEW IF EXISTS imdb.q10c_company_name CASCADE;
DROP VIEW IF EXISTS imdb.q10c_title CASCADE;

CREATE VIEW imdb.q10c_cast_info as
SELECT person_id, movie_id, person_role_id, role_id
FROM imdb.cast_info as ci
WHERE ci.note LIKE '%(producer)%';

CREATE VIEW imdb.q10c_company_name as
SELECT company_id
FROM imdb.company_name as cn
WHERE cn.country_code = '[us]';

CREATE VIEW imdb.q10c_title as
SELECT movie_id, kind_id
FROM imdb.title as t
WHERE t.production_year > 1990;

END;