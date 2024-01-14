-- implement the select predicates as views on top of the tables for JOB 28a
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/28a.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q28a_comp_cast_type1 CASCADE;
DROP VIEW IF EXISTS imdb.q28a_comp_cast_type2 CASCADE;
DROP VIEW IF EXISTS imdb.q28a_company_name CASCADE;
DROP VIEW IF EXISTS imdb.q28a_info_type1 CASCADE;
DROP VIEW IF EXISTS imdb.q28a_info_type2 CASCADE;
DROP VIEW IF EXISTS imdb.q28a_keyword CASCADE;
DROP VIEW IF EXISTS imdb.q28a_kind_type CASCADE;
DROP VIEW IF EXISTS imdb.q28a_movie_companies CASCADE;
DROP VIEW IF EXISTS imdb.q28a_movie_info CASCADE;
DROP VIEW IF EXISTS imdb.q28a_movie_info_idx2 CASCADE;
DROP VIEW IF EXISTS imdb.q28a_title CASCADE;


CREATE VIEW imdb.q28a_comp_cast_type1 as
SELECT subject_id
FROM imdb.comp_cast_type as cct1
WHERE cct1.kind = 'crew';

CREATE VIEW imdb.q28a_comp_cast_type2 as
SELECT subject_id as status_id
FROM imdb.comp_cast_type as cct2
WHERE cct2.kind != 'complete+verified';

CREATE VIEW imdb.q28a_company_name as
SELECT company_id
FROM imdb.company_name as cn
WHERE cn.country_code != '[us]';

CREATE VIEW imdb.q28a_info_type1 as
SELECT info_type_id
FROM imdb.info_type as it1
WHERE it1.info = 'countries';

CREATE VIEW imdb.q28a_info_type2 as
SELECT info_type_id as info_type_id2
FROM imdb.info_type as it2
WHERE it2.info = 'rating';

CREATE VIEW imdb.q28a_keyword as
SELECT keyword_id
FROM imdb.keyword as k
WHERE k.keyword IN ('murder',
                    'murder-in-title',
                    'blood',
                    'violence');

CREATE VIEW imdb.q28a_kind_type as
SELECT kind_id
FROM imdb.kind_type as kt
WHERE kt.kind IN ('movie',
                  'episode');

CREATE VIEW imdb.q28a_movie_companies as
SELECT movie_id, company_id, company_type_id
FROM imdb.movie_companies as mc
WHERE mc.note NOT LIKE '%(USA)%'
  AND mc.note LIKE '%(200%)%';

CREATE VIEW imdb.q28a_movie_info as
SELECT movie_id, info_type_id
FROM imdb.movie_info as mi
WHERE mi.info IN ('Sweden',
                  'Norway',
                  'Germany',
                  'Denmark',
                  'Swedish',
                  'Danish',
                  'Norwegian',
                  'German',
                  'USA',
                  'American');

CREATE VIEW imdb.q28a_movie_info_idx2 as
SELECT movie_id, info_type_id as info_type_id2
FROM imdb.movie_info_idx as mi_idx
WHERE mi_idx.info < '8.5';

CREATE VIEW imdb.q28a_title as
SELECT movie_id, kind_id
FROM imdb.title as t
WHERE t.production_year > 2000;

END;