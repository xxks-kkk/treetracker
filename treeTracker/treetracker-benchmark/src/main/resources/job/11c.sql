-- implement the select predicates as views on top of the tables for JOB 11c
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/11c.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q11c_company_type CASCADE;
DROP VIEW IF EXISTS imdb.q11c_company_name CASCADE;
DROP VIEW IF EXISTS imdb.q11c_keyword CASCADE;
DROP VIEW IF EXISTS imdb.q11c_movie_companies CASCADE;
DROP VIEW IF EXISTS imdb.q11c_title CASCADE;

CREATE VIEW imdb.q11c_company_type as
SELECT company_type_id
FROM imdb.company_type as ct
WHERE ct.kind != 'production companies' AND ct.kind IS NOT NULL;

CREATE VIEW imdb.q11c_company_name as
SELECT company_id
FROM imdb.company_name as cn
WHERE cn.country_code !='[pl]' AND (cn.name LIKE '20th Century Fox%' OR cn.name LIKE 'Twentieth Century Fox%');

CREATE VIEW imdb.q11c_keyword as
SELECT keyword_id
FROM imdb.keyword as k
WHERE k.keyword IN ('sequel', 'revenge', 'based-on-novel');

CREATE VIEW imdb.q11c_movie_companies as
SELECT movie_id, company_id, company_type_id
FROM imdb.movie_companies as mc
WHERE mc.note IS NOT NULL;

CREATE VIEW imdb.q11c_title as
SELECT movie_id, kind_id
FROM imdb.title as t
WHERE t.production_year > 1950;

END;