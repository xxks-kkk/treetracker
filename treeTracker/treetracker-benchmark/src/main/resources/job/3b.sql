-- implement the select predicates as views on top of the tables for JOB 3b
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/3b.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q3b_keyword CASCADE;
DROP VIEW IF EXISTS imdb.q3b_movie_info CASCADE;
DROP VIEW IF EXISTS imdb.q3b_title CASCADE;


CREATE VIEW imdb.q3b_keyword as
SELECT keyword_id
FROM imdb.keyword as k
WHERE k.keyword LIKE '%sequel%';

CREATE VIEW imdb.q3b_movie_info as
SELECT movie_id, info_type_id
FROM imdb.movie_info as mi
WHERE mi.info IN ('Bulgaria');

CREATE VIEW imdb.q3b_title as
SELECT movie_id, kind_id
FROM imdb.title as t
WHERE t.production_year > 2010;


END