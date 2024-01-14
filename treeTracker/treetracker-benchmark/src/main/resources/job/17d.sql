-- implement the select predicates as views on top of the tables for JOB 17d
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/17d.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q17d_keyword CASCADE;
DROP VIEW IF EXISTS imdb.q17d_name CASCADE;

CREATE VIEW imdb.q17d_keyword as
SELECT keyword_id
FROM imdb.keyword as k
WHERE k.keyword ='character-name-in-title';

CREATE VIEW imdb.q17d_name as
SELECT person_id
FROM imdb.name as n
WHERE n.name LIKE '%Bert%';

END;