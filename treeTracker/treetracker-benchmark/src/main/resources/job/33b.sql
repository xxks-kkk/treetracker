-- implement the select predicates as views on top of the tables for JOB 33b
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/33b.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q33b_company_name1 CASCADE;
DROP VIEW IF EXISTS imdb.q33b_company_name2 CASCADE;
DROP VIEW IF EXISTS imdb.q33b_info_type1 CASCADE;
DROP VIEW IF EXISTS imdb.q33b_info_type2 CASCADE;
DROP VIEW IF EXISTS imdb.q33b_kind_type1 CASCADE;
DROP VIEW IF EXISTS imdb.q33b_kind_type2 CASCADE;
DROP VIEW IF EXISTS imdb.q33b_link_type CASCADE;
DROP VIEW IF EXISTS imdb.q33b_movie_info_idx1 CASCADE;
DROP VIEW IF EXISTS imdb.q33b_movie_info_idx2 CASCADE;
DROP VIEW IF EXISTS imdb.q33b_title1 CASCADE;
DROP VIEW IF EXISTS imdb.q33b_title2 CASCADE;
DROP VIEW IF EXISTS imdb.q33b_movie_companies1 CASCADE;
DROP VIEW IF EXISTS imdb.q33b_movie_companies2 CASCADE;

CREATE VIEW imdb.q33b_company_name1 as
SELECT company_id
FROM imdb.company_name as cn1
WHERE cn1.country_code = '[nl]';

CREATE VIEW imdb.q33b_company_name2 as
SELECT company_id as company_id2
FROM imdb.company_name;

CREATE VIEW imdb.q33b_info_type1 as
SELECT info_type_id
FROM imdb.info_type as it1
WHERE it1.info = 'rating';

CREATE VIEW imdb.q33b_info_type2 as
SELECT info_type_id as info_type_id2
FROM imdb.info_type as it2
WHERE it2.info = 'rating';

CREATE VIEW imdb.q33b_kind_type1 as
SELECT kind_id
FROM imdb.kind_type as kt1
WHERE kt1.kind IN ('tv series');

CREATE VIEW imdb.q33b_kind_type2 as
SELECT kind_id as kind_id2
FROM imdb.kind_type as kt2
WHERE kt2.kind IN ('tv series');

CREATE VIEW imdb.q33b_link_type as
SELECT link_type_id
FROM imdb.link_type as lt
WHERE lt.link LIKE '%follow%';

CREATE VIEW imdb.q33b_movie_info_idx1 as
SELECT movie_id, info_type_id
FROM imdb.movie_info_idx;

CREATE VIEW imdb.q33b_movie_info_idx2 as
SELECT movie_id as linked_movie_id, info_type_id as info_type_id2
FROM imdb.movie_info_idx as mi_idx2
WHERE mi_idx2.info < '3.0';

CREATE VIEW imdb.q33b_title1 as
SELECT movie_id, kind_id
FROM imdb.title;

CREATE VIEW imdb.q33b_title2 as
SELECT movie_id as linked_movie_id, kind_id as kind_id2
FROM imdb.title as t2
WHERE t2.production_year = 2007;

CREATE VIEW imdb.q33b_movie_companies1 as
SELECT movie_id, company_id, company_type_id
FROM imdb.movie_companies;

CREATE VIEW imdb.q33b_movie_companies2 as
SELECT movie_id as linked_movie_id, company_id as company_id2, company_type_id as company_type_id2
FROM imdb.movie_companies;

END;