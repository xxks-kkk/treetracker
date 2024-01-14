-- implement the select predicates as views on top of the tables for JOB 19d
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/19d.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q19d_cast_info CASCADE;
DROP VIEW IF EXISTS imdb.q19d_company_name CASCADE;
DROP VIEW IF EXISTS imdb.q19d_info_type CASCADE;
DROP VIEW IF EXISTS imdb.q19d_name CASCADE;
DROP VIEW IF EXISTS imdb.q19d_role_type CASCADE;
DROP VIEW IF EXISTS imdb.q19d_title CASCADE;

CREATE VIEW imdb.q19d_cast_info as
SELECT person_id, movie_id, person_role_id, role_id
FROM imdb.cast_info as ci
WHERE ci.note IN ('(voice)',
                  '(voice: Japanese version)',
                  '(voice) (uncredited)',
                  '(voice: English version)');

CREATE VIEW imdb.q19d_company_name as
SELECT company_id
FROM imdb.company_name as cn
WHERE cn.country_code = '[us]';

CREATE VIEW imdb.q19d_info_type as
SELECT info_type_id
FROM imdb.info_type as it
WHERE it.info = 'release dates';

CREATE VIEW imdb.q19d_name as
SELECT person_id
FROM imdb.name as n
WHERE n.gender ='f';

CREATE VIEW imdb.q19d_role_type as
SELECT role_id
FROM imdb.role_type as rt
WHERE rt.role ='actress';

CREATE VIEW imdb.q19d_title as
SELECT movie_id, kind_id
FROM imdb.title as t
WHERE t.production_year > 2000;

END;