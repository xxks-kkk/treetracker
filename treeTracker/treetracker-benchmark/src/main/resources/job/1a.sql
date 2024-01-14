-- implement the select predicates as views on top of the tables for JOB 1a
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/1a.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q1a_company_type CASCADE;
DROP VIEW IF EXISTS imdb.q1a_info_type CASCADE;
DROP VIEW IF EXISTS imdb.q1a_movie_companies CASCADE;

CREATE VIEW imdb.q1a_company_type as
SELECT company_type_id
FROM imdb.company_type as ct
WHERE ct.kind = 'production companies';

CREATE VIEW imdb.q1a_info_type as
SELECT info_type_id
FROM imdb.info_type as it
WHERE it.info = 'top 250 rank';

CREATE VIEW imdb.q1a_movie_companies as
SELECT movie_id, company_id, company_type_id
FROM imdb.movie_companies as mc
WHERE mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%' AND (mc.note LIKE '%(co-production)%' OR mc.note LIKE '%(presents)%');

END;