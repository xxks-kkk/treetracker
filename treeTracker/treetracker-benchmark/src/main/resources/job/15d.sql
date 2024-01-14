-- implement the select predicates as views on top of the tables for JOB 15d
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/15d.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q15d_company_name CASCADE;
DROP VIEW IF EXISTS imdb.q15d_info_type CASCADE;
DROP VIEW IF EXISTS imdb.q15d_movie_info CASCADE;
DROP VIEW IF EXISTS imdb.q15d_title CASCADE;

CREATE VIEW imdb.q15d_company_name as
SELECT company_id
FROM imdb.company_name as cn
WHERE cn.country_code = '[us]';

CREATE VIEW imdb.q15d_info_type as
SELECT info_type_id
FROM imdb.info_type as it
WHERE it.info = 'release dates';

CREATE VIEW imdb.q15d_movie_info as
SELECT movie_id, info_type_id
FROM imdb.movie_info as mi
WHERE mi.note LIKE '%internet%';

CREATE VIEW imdb.q15d_title as
SELECT movie_id, kind_id
FROM imdb.title as t
WHERE t.production_year > 1990;

END;