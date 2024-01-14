-- implement the select predicates as views on top of the tables for JOB 1c
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/1c.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q1c_company_type CASCADE;
DROP VIEW IF EXISTS imdb.q1c_info_type CASCADE;
DROP VIEW IF EXISTS imdb.q1c_movie_companies CASCADE;
DROP VIEW IF EXISTS imdb.q1c_title CASCADE;

CREATE VIEW imdb.q1c_company_type as
SELECT company_type_id
FROM imdb.company_type as ct
WHERE ct.kind = 'production companies';

CREATE VIEW imdb.q1c_info_type as
SELECT info_type_id
FROM imdb.info_type as it
WHERE it.info = 'top 250 rank';

CREATE VIEW imdb.q1c_movie_companies as
SELECT movie_id, company_id, company_type_id
FROM imdb.movie_companies as mc
WHERE mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%'
  AND (mc.note LIKE '%(co-production)%');

CREATE VIEW imdb.q1c_title as
SELECT movie_id, kind_id
FROM imdb.title as t
WHERE t.production_year > 2010;



END;