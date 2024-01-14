-- implement the select predicates as views on top of the tables for JOB 17b
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/17b.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q17b_keyword CASCADE;
DROP VIEW IF EXISTS imdb.q17b_name CASCADE;

CREATE VIEW imdb.q17b_keyword as
SELECT keyword_id
FROM imdb.keyword as k
WHERE k.keyword ='character-name-in-title';

CREATE VIEW imdb.q17b_name as
SELECT person_id
FROM imdb.name as n
WHERE n.name LIKE 'Z%';

END;