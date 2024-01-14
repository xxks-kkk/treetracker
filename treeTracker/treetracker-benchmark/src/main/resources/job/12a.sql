-- implement the select predicates as views on top of the tables for JOB 12a
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/12a.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q12a_company_type CASCADE;
DROP VIEW IF EXISTS imdb.q12a_company_name CASCADE;
DROP VIEW IF EXISTS imdb.q12a_info_type1 CASCADE;
DROP VIEW IF EXISTS imdb.q12a_info_type2 CASCADE;
DROP VIEW IF EXISTS imdb.q12a_movie_info CASCADE;
DROP VIEW IF EXISTS imdb.q12a_movie_info_idx2 CASCADE;
DROP VIEW IF EXISTS imdb.q12a_title CASCADE;

CREATE VIEW imdb.q12a_company_type as
SELECT company_type_id
FROM imdb.company_type as ct
WHERE ct.kind = 'production companies';

CREATE VIEW imdb.q12a_company_name as
SELECT company_id
FROM imdb.company_name as cn
WHERE cn.country_code = '[us]';

CREATE VIEW imdb.q12a_info_type1 as
SELECT info_type_id
FROM imdb.info_type as it1
WHERE it1.info = 'genres';

CREATE VIEW imdb.q12a_info_type2 as
SELECT info_type_id as info_type_id2
FROM imdb.info_type as it2
WHERE it2.info = 'rating';

CREATE VIEW imdb.q12a_movie_info as
SELECT movie_id, info_type_id
FROM imdb.movie_info as mi
WHERE mi.info IN ('Drama', 'Horror');

CREATE VIEW imdb.q12a_movie_info_idx2 as
SELECT movie_id, info_type_id as info_type_id2
FROM imdb.movie_info_idx as mi_idx
WHERE mi_idx.info > '8.0';

CREATE VIEW imdb.q12a_title as
SELECT movie_id, kind_id
FROM imdb.title as t
WHERE t.production_year BETWEEN 2005 AND 2008;

END;