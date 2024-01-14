-- implement the select predicates as views on top of the tables for JOB 4b
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/4b.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q4b_info_type CASCADE;
DROP VIEW IF EXISTS imdb.q4b_keyword CASCADE;
DROP VIEW IF EXISTS imdb.q4b_movie_info_idx CASCADE;
DROP VIEW IF EXISTS imdb.q4b_title CASCADE;

CREATE VIEW imdb.q4b_info_type as
SELECT info_type_id
FROM imdb.info_type as it
WHERE it.info ='rating';

CREATE VIEW imdb.q4b_keyword as
SELECT keyword_id
FROM imdb.keyword as k
WHERE k.keyword LIKE '%sequel%';

CREATE VIEW imdb.q4b_movie_info_idx as
SELECT movie_id, info_type_id
FROM imdb.movie_info_idx as mi_idx
WHERE mi_idx.info > '9.0';

CREATE VIEW imdb.q4b_title as
SELECT movie_id, kind_id
FROM imdb.title as t
WHERE t.production_year > 2010;


END