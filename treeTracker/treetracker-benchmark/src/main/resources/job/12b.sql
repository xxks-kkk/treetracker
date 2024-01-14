-- implement the select predicates as views on top of the tables for JOB 12b
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/12b.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q12b_company_type CASCADE;
DROP VIEW IF EXISTS imdb.q12b_company_name CASCADE;
DROP VIEW IF EXISTS imdb.q12b_info_type1 CASCADE;
DROP VIEW IF EXISTS imdb.q12b_info_type2 CASCADE;
DROP VIEW IF EXISTS imdb.q12b_title CASCADE;
DROP VIEW IF EXISTS imdb.q12b_movie_info_idx2 CASCADE;

CREATE VIEW imdb.q12b_company_type as
SELECT company_type_id
FROM imdb.company_type as ct
WHERE ct.kind IS NOT NULL AND (ct.kind ='production companies' OR ct.kind = 'distributors');

CREATE VIEW imdb.q12b_company_name as
SELECT company_id
FROM imdb.company_name as cn
WHERE cn.country_code ='[us]';

CREATE VIEW imdb.q12b_info_type1 as
SELECT info_type_id
FROM imdb.info_type as it1
WHERE it1.info ='budget';

CREATE VIEW imdb.q12b_info_type2 as
SELECT info_type_id as info_type_id2
FROM imdb.info_type as it2
WHERE it2.info ='bottom 10 rank';

CREATE VIEW imdb.q12b_title as
SELECT movie_id, kind_id
FROM imdb.title as t
WHERE t.production_year >2000 AND (t.title LIKE 'Birdemic%' OR t.title LIKE '%Movie%');

CREATE VIEW imdb.q12b_movie_info_idx2 as
SELECT movie_id, info_type_id as info_type_id2
FROM imdb.movie_info_idx;

END;