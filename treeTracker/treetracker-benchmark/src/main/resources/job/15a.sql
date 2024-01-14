-- implement the select predicates as views on top of the tables for JOB 15a
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/15a.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q15a_company_name CASCADE;
DROP VIEW IF EXISTS imdb.q15a_info_type CASCADE;
DROP VIEW IF EXISTS imdb.q15a_movie_companies CASCADE;
DROP VIEW IF EXISTS imdb.q15a_movie_info CASCADE;
DROP VIEW IF EXISTS imdb.q15a_title CASCADE;

CREATE VIEW imdb.q15a_company_name as
SELECT company_id
FROM imdb.company_name as cn
WHERE cn.country_code = '[us]';

CREATE VIEW imdb.q15a_info_type as
SELECT info_type_id
FROM imdb.info_type as it
WHERE it.info = 'release dates';

CREATE VIEW imdb.q15a_movie_companies as
SELECT movie_id, company_id, company_type_id
FROM imdb.movie_companies as mc
WHERE mc.note LIKE '%(200%)%' AND mc.note LIKE '%(worldwide)%';

CREATE VIEW imdb.q15a_movie_info as
SELECT movie_id, info_type_id
FROM imdb.movie_info as mi
WHERE mi.note LIKE '%internet%' AND mi.info LIKE 'USA:% 200%';

CREATE VIEW imdb.q15a_title as
SELECT movie_id, kind_id
FROM imdb.title as t
WHERE t.production_year > 2000;

END;