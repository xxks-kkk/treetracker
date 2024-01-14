-- implement the select predicates as views on top of the tables for JOB 16a
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/16a.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q16a_company_name CASCADE;
DROP VIEW IF EXISTS imdb.q16a_keyword CASCADE;
DROP VIEW IF EXISTS imdb.q16a_title CASCADE;

CREATE VIEW imdb.q16a_company_name as
SELECT company_id
FROM imdb.company_name as cn
WHERE cn.country_code = '[us]';

CREATE VIEW imdb.q16a_keyword as
SELECT keyword_id
FROM imdb.keyword as k
WHERE k.keyword ='character-name-in-title';

CREATE VIEW imdb.q16a_title as
SELECT movie_id, kind_id
FROM imdb.title as t
WHERE t.episode_nr >= 50 AND t.episode_nr < 100;

END;