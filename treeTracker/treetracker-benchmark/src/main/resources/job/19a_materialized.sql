-- implement the select predicates as views on top of the tables for JOB 19a
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/19a.sql

BEGIN TRANSACTION;

DROP MATERIALIZED VIEW IF EXISTS imdb.q19a_cast_info CASCADE;
DROP MATERIALIZED VIEW IF EXISTS imdb.q19a_company_name CASCADE;
DROP MATERIALIZED VIEW IF EXISTS imdb.q19a_info_type CASCADE;
DROP MATERIALIZED VIEW IF EXISTS imdb.q19a_movie_companies CASCADE;
DROP MATERIALIZED VIEW IF EXISTS imdb.q19a_movie_info CASCADE;
DROP MATERIALIZED VIEW IF EXISTS imdb.q19a_name CASCADE;
DROP MATERIALIZED VIEW IF EXISTS imdb.q19a_role_type CASCADE;
DROP MATERIALIZED VIEW IF EXISTS imdb.q19a_title CASCADE;

CREATE MATERIALIZED VIEW imdb.q19a_cast_info as
SELECT person_id, movie_id, person_role_id, role_id
FROM imdb.cast_info as ci
WHERE ci.note IN ('(voice)', '(voice: Japanese version)', '(voice) (uncredited)', '(voice: English version)');

CREATE MATERIALIZED VIEW imdb.q19a_company_name as
SELECT company_id
FROM imdb.company_name as cn
WHERE cn.country_code = '[us]';

CREATE MATERIALIZED VIEW imdb.q19a_info_type as
SELECT info_type_id
FROM imdb.info_type as it
WHERE it.info = 'release dates';

CREATE MATERIALIZED VIEW imdb.q19a_movie_companies as
SELECT movie_id, company_id, company_type_id
FROM imdb.movie_companies as mc
WHERE mc.note IS NOT NULL AND (mc.note LIKE '%(USA)%' OR mc.note LIKE '%(worldwide)%');

CREATE MATERIALIZED VIEW imdb.q19a_movie_info as
SELECT movie_id, info_type_id
FROM imdb.movie_info as mi
WHERE mi.info IS NOT NULL AND (mi.info LIKE 'Japan:%200%' OR mi.info LIKE 'USA:%200%');

CREATE MATERIALIZED VIEW imdb.q19a_name as
SELECT person_id
FROM imdb.name as n
WHERE n.gender ='f' AND n.name LIKE '%Ang%';

CREATE MATERIALIZED VIEW imdb.q19a_role_type as
SELECT role_id
FROM imdb.role_type as rt
WHERE rt.role ='actress';

CREATE MATERIALIZED VIEW imdb.q19a_title as
SELECT movie_id, kind_id
FROM imdb.title as t
WHERE t.production_year BETWEEN 2005 AND 2009;

ANALYZE imdb.q19a_cast_info;
ANALYZE imdb.q19a_company_name;
ANALYZE imdb.q19a_info_type;
ANALYZE imdb.q19a_movie_companies;
ANALYZE imdb.q19a_movie_info;
ANALYZE imdb.q19a_name;
ANALYZE imdb.q19a_role_type;
ANALYZE imdb.q19a_title;

END;