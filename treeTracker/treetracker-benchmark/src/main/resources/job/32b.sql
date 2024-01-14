-- implement the select predicates as views on top of the tables for JOB 32b
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/32b.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q32b_keyword CASCADE;
DROP VIEW IF EXISTS imdb.q32b_title1 CASCADE;
DROP VIEW IF EXISTS imdb.q32b_title2 CASCADE;

CREATE VIEW imdb.q32b_keyword as
SELECT keyword_id
FROM imdb.keyword as k
WHERE k.keyword ='character-name-in-title';

CREATE VIEW imdb.q32b_title1 as
SELECT movie_id, kind_id
FROM imdb.title;

CREATE VIEW imdb.q32b_title2 as
SELECT movie_id as linked_movie_id, kind_id as kind_id1
FROM imdb.title;

END