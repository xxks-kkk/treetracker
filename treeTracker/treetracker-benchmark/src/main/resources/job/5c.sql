-- implement the select predicates as views on top of the tables for JOB 5c
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/5c.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q5c_company_type CASCADE;
DROP VIEW IF EXISTS imdb.q5c_movie_companies CASCADE;
DROP VIEW IF EXISTS imdb.q5c_movie_info CASCADE;
DROP VIEW IF EXISTS imdb.q5c_title CASCADE;

CREATE VIEW imdb.q5c_company_type as
SELECT company_type_id
FROM imdb.company_type as ct
WHERE ct.kind = 'production companies';

CREATE VIEW imdb.q5c_movie_companies as
SELECT movie_id, company_id, company_type_id
FROM imdb.movie_companies as mc
WHERE mc.note NOT LIKE '%(TV)%'
  AND mc.note LIKE '%(USA)%';

CREATE VIEW imdb.q5c_movie_info as
SELECT movie_id, info_type_id
FROM imdb.movie_info as mi
WHERE mi.info IN ('Sweden',
                  'Norway',
                  'Germany',
                  'Denmark',
                  'Swedish',
                  'Denish',
                  'Norwegian',
                  'German',
                  'USA',
                  'American');

CREATE VIEW imdb.q5c_title as
SELECT movie_id, kind_id
FROM imdb.title as t
WHERE t.production_year > 1990;


END