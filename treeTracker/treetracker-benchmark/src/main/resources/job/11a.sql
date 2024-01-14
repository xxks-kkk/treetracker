-- implement the select predicates as views on top of the tables for JOB 11a
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/11a.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q11a_company_type CASCADE;
DROP VIEW IF EXISTS imdb.q11a_company_name CASCADE;
DROP VIEW IF EXISTS imdb.q11a_keyword CASCADE;
DROP VIEW IF EXISTS imdb.q11a_link_type CASCADE;
DROP VIEW IF EXISTS imdb.q11a_movie_companies CASCADE;
DROP VIEW IF EXISTS imdb.q11a_title CASCADE;

CREATE VIEW imdb.q11a_company_type as
SELECT company_type_id
FROM imdb.company_type as ct
WHERE ct.kind ='production companies';

CREATE VIEW imdb.q11a_company_name as
SELECT company_id
FROM imdb.company_name as cn
WHERE cn.country_code !='[pl]' AND (cn.name LIKE '%Film%' OR cn.name LIKE '%Warner%');

CREATE VIEW imdb.q11a_keyword as
SELECT keyword_id
FROM imdb.keyword as k
WHERE k.keyword ='sequel';

CREATE VIEW imdb.q11a_link_type as
SELECT link_type_id
FROM imdb.link_type as lt
WHERE lt.link LIKE '%follow%';

CREATE VIEW imdb.q11a_movie_companies as
SELECT movie_id, company_id, company_type_id
FROM imdb.movie_companies as mc
WHERE mc.note IS NULL;

CREATE VIEW imdb.q11a_title as
SELECT movie_id, kind_id
FROM imdb.title as t
WHERE t.production_year BETWEEN 1950 AND 2000;

END;