-- implement the select predicates as views on top of the tables for JOB 13b
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/13b.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q13b_company_type CASCADE;
DROP VIEW IF EXISTS imdb.q13b_company_name CASCADE;
DROP VIEW IF EXISTS imdb.q13b_info_type1 CASCADE;
DROP VIEW IF EXISTS imdb.q13b_info_type2 CASCADE;
DROP VIEW IF EXISTS imdb.q13b_kind_type CASCADE;
DROP VIEW IF EXISTS imdb.q13b_title CASCADE;
DROP VIEW IF EXISTS imdb.q13b_movie_info CASCADE;

CREATE VIEW imdb.q13b_company_type as
SELECT company_type_id
FROM imdb.company_type as ct
WHERE ct.kind ='production companies';

CREATE VIEW imdb.q13b_company_name as
SELECT company_id
FROM imdb.company_name as cn
WHERE cn.country_code ='[us]';

CREATE VIEW imdb.q13b_info_type1 as
SELECT info_type_id
FROM imdb.info_type as it1
WHERE it1.info ='rating';

CREATE VIEW imdb.q13b_info_type2 as
SELECT info_type_id as info_type_id2
FROM imdb.info_type as it2
WHERE it2.info ='release dates';

CREATE VIEW imdb.q13b_kind_type as
SELECT kind_id
FROM imdb.kind_type as kt
WHERE kt.kind ='movie';

CREATE VIEW imdb.q13b_title as
SELECT movie_id, kind_id
FROM imdb.title as t
WHERE t.title != '' AND (t.title LIKE '%Champion%' OR t.title LIKE '%Loser%');

CREATE VIEW imdb.q13b_movie_info as
SELECT movie_id, info_type_id as info_type_id2
FROM imdb.movie_info;

END;