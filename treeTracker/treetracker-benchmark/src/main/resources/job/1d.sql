-- implement the select predicates as views on top of the tables for JOB 1d
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/1d.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q1d_company_type CASCADE;
DROP VIEW IF EXISTS imdb.q1d_info_type CASCADE;
DROP VIEW IF EXISTS imdb.q1d_movie_companies CASCADE;
DROP VIEW IF EXISTS imdb.q1d_title CASCADE;

CREATE VIEW imdb.q1d_company_type as
SELECT company_type_id
FROM imdb.company_type as ct
WHERE ct.kind = 'production companies';

CREATE VIEW imdb.q1d_info_type as
SELECT info_type_id
FROM imdb.info_type as it
WHERE it.info = 'bottom 10 rank';

CREATE VIEW imdb.q1d_movie_companies as
SELECT movie_id, company_id, company_type_id
FROM imdb.movie_companies as mc
WHERE mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%';

CREATE VIEW imdb.q1d_title as
SELECT movie_id, kind_id
FROM imdb.title as t
WHERE t.production_year > 2000;



END;