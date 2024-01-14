-- implement the select predicates as views on top of the tables for JOB 6e
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/6e.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q6e_keyword CASCADE;
DROP VIEW IF EXISTS imdb.q6e_name CASCADE;
DROP VIEW IF EXISTS imdb.q6e_title CASCADE;

CREATE VIEW imdb.q6e_keyword as
SELECT keyword_id
FROM imdb.keyword as k
WHERE k.keyword = 'marvel-cinematic-universe';

CREATE VIEW imdb.q6e_name as
SELECT person_id
FROM imdb.name as n
WHERE n.name LIKE '%Downey%Robert%';

CREATE VIEW imdb.q6e_title as
SELECT movie_id, kind_id
FROM imdb.title as t
WHERE t.production_year > 2000;


END