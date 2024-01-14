-- implement the select predicates as views on top of the tables for JOB 10a
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/10a.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q10a_cast_info CASCADE;
DROP VIEW IF EXISTS imdb.q10a_company_name CASCADE;
DROP VIEW IF EXISTS imdb.q10a_role_type CASCADE;
DROP VIEW IF EXISTS imdb.q10a_title CASCADE;

CREATE VIEW imdb.q10a_cast_info as
SELECT person_id, movie_id, person_role_id, role_id
FROM imdb.cast_info as ci
WHERE ci.note LIKE '%(voice)%'
  AND ci.note LIKE '%(uncredited)%';

CREATE VIEW imdb.q10a_company_name as
SELECT company_id
FROM imdb.company_name as cn
WHERE cn.country_code = '[ru]';

CREATE VIEW imdb.q10a_role_type as
SELECT role_id
FROM imdb.role_type as rt
WHERE rt.role = 'actor';

CREATE VIEW imdb.q10a_title as
SELECT movie_id, kind_id
FROM imdb.title as t
WHERE t.production_year > 2005;

END;