-- implement the select predicates as views on top of the tables for JOB 26b
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/26b.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q26b_comp_cast_type1 CASCADE;
DROP VIEW IF EXISTS imdb.q26b_comp_cast_type2 CASCADE;
DROP VIEW IF EXISTS imdb.q26b_char_name CASCADE;
DROP VIEW IF EXISTS imdb.q26b_info_type CASCADE;
DROP VIEW IF EXISTS imdb.q26b_keyword CASCADE;
DROP VIEW IF EXISTS imdb.q26b_kind_type CASCADE;
DROP VIEW IF EXISTS imdb.q26b_movie_info_idx CASCADE;
DROP VIEW IF EXISTS imdb.q26b_title CASCADE;


CREATE VIEW imdb.q26b_comp_cast_type1 as
SELECT subject_id
FROM imdb.comp_cast_type as cct1
WHERE cct1.kind = 'cast';

CREATE VIEW imdb.q26b_comp_cast_type2 as
SELECT subject_id as status_id
FROM imdb.comp_cast_type as cct2
WHERE cct2.kind LIKE '%complete%';

CREATE VIEW imdb.q26b_char_name as
SELECT person_role_id
FROM imdb.char_name as chn
WHERE chn.name IS NOT NULL
  AND (chn.name LIKE '%man%'
    OR chn.name LIKE '%Man%');

CREATE VIEW imdb.q26b_info_type as
SELECT info_type_id
FROM imdb.info_type as it
WHERE it.info = 'rating';

CREATE VIEW imdb.q26b_keyword as
SELECT keyword_id
FROM imdb.keyword as k
WHERE k.keyword IN ('superhero',
                    'marvel-comics',
                    'based-on-comic',
                    'fight');

CREATE VIEW imdb.q26b_kind_type as
SELECT kind_id
FROM imdb.kind_type as kt
WHERE kt.kind = 'movie';

CREATE VIEW imdb.q26b_movie_info_idx as
SELECT movie_id, info_type_id
FROM imdb.movie_info_idx as mi_idx
WHERE mi_idx.info > '8.0';

CREATE VIEW imdb.q26b_title as
SELECT movie_id, kind_id
FROM imdb.title as t
WHERE t.production_year > 2005;

END;