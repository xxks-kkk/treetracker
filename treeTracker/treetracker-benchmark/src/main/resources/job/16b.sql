-- implement the select predicates as views on top of the tables for JOB 16b
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/16b.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q16b_company_name CASCADE;
DROP VIEW IF EXISTS imdb.q16b_keyword CASCADE;

CREATE VIEW imdb.q16b_company_name as
SELECT company_id
FROM imdb.company_name as cn
WHERE cn.country_code = '[us]';

CREATE VIEW imdb.q16b_keyword as
SELECT keyword_id
FROM imdb.keyword as k
WHERE k.keyword ='character-name-in-title';

END;