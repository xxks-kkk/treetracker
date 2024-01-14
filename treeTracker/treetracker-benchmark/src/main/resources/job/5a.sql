-- implement the select predicates as views on top of the tables for JOB 5a
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/5a.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q5a_company_type CASCADE;
DROP VIEW IF EXISTS imdb.q5a_movie_companies CASCADE;
DROP VIEW IF EXISTS imdb.q5a_movie_info CASCADE;
DROP VIEW IF EXISTS imdb.q5a_title CASCADE;

CREATE VIEW imdb.q5a_company_type as
SELECT company_type_id
FROM imdb.company_type as ct
WHERE ct.kind = 'production companies';

CREATE VIEW imdb.q5a_movie_companies as
SELECT movie_id, company_id, company_type_id
FROM imdb.movie_companies as mc
WHERE mc.note LIKE '%(theatrical)%'
  AND mc.note LIKE '%(France)%';

CREATE VIEW imdb.q5a_movie_info as
SELECT movie_id, info_type_id
FROM imdb.movie_info as mi
WHERE mi.info IN ('Sweden',
                  'Norway',
                  'Germany',
                  'Denmark',
                  'Swedish',
                  'Denish',
                  'Norwegian',
                  'German');

CREATE VIEW imdb.q5a_title as
SELECT movie_id, kind_id
FROM imdb.title as t
WHERE t.production_year > 2005;


END