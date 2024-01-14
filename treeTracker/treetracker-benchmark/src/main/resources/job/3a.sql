-- implement the select predicates as views on top of the tables for JOB 3a
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/3a.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q3a_keyword CASCADE;
DROP VIEW IF EXISTS imdb.q3a_movie_info CASCADE;
DROP VIEW IF EXISTS imdb.q3a_title CASCADE;


CREATE VIEW imdb.q3a_keyword as
SELECT keyword_id
FROM imdb.keyword as k
WHERE k.keyword LIKE '%sequel%';

CREATE VIEW imdb.q3a_movie_info as
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

CREATE VIEW imdb.q3a_title as
SELECT movie_id, kind_id
FROM imdb.title as t
WHERE t.production_year > 2005;


END