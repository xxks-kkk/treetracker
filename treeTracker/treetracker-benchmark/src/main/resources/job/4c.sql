-- implement the select predicates as views on top of the tables for JOB 4c
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/4c.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q4c_info_type CASCADE;
DROP VIEW IF EXISTS imdb.q4c_keyword CASCADE;
DROP VIEW IF EXISTS imdb.q4c_movie_info_idx CASCADE;
DROP VIEW IF EXISTS imdb.q4c_title CASCADE;

CREATE VIEW imdb.q4c_info_type as
SELECT info_type_id
FROM imdb.info_type as it
WHERE it.info ='rating';

CREATE VIEW imdb.q4c_keyword as
SELECT keyword_id
FROM imdb.keyword as k
WHERE k.keyword LIKE '%sequel%';

CREATE VIEW imdb.q4c_movie_info_idx as
SELECT movie_id, info_type_id
FROM imdb.movie_info_idx as mi_idx
WHERE mi_idx.info > '2.0';

CREATE VIEW imdb.q4c_title as
SELECT movie_id, kind_id
FROM imdb.title as t
WHERE t.production_year > 1990;


END