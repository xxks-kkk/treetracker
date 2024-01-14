-- implement the select predicates as views on top of the tables for JOB 17a
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/17a.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q17a_company_name CASCADE;
DROP VIEW IF EXISTS imdb.q17a_keyword CASCADE;
DROP VIEW IF EXISTS imdb.q17a_name CASCADE;

CREATE VIEW imdb.q17a_company_name as
SELECT company_id
FROM imdb.company_name as cn
WHERE cn.country_code = '[us]';

CREATE VIEW imdb.q17a_keyword as
SELECT keyword_id
FROM imdb.keyword as k
WHERE k.keyword ='character-name-in-title';

CREATE VIEW imdb.q17a_name as
SELECT person_id
FROM imdb.name as n
WHERE n.name LIKE 'B%';

END;