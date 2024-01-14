-- implement the select predicates as views on top of the tables for JOB 29a
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/29a.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q29a_comp_cast_type1 CASCADE;
DROP VIEW IF EXISTS imdb.q29a_comp_cast_type2 CASCADE;
DROP VIEW IF EXISTS imdb.q29a_char_name CASCADE;
DROP VIEW IF EXISTS imdb.q29a_cast_info CASCADE;
DROP VIEW IF EXISTS imdb.q29a_company_name CASCADE;
DROP VIEW IF EXISTS imdb.q29a_info_type1 CASCADE;
DROP VIEW IF EXISTS imdb.q29a_info_type2 CASCADE;
DROP VIEW IF EXISTS imdb.q29a_keyword CASCADE;
DROP VIEW IF EXISTS imdb.q29a_movie_info CASCADE;
DROP VIEW IF EXISTS imdb.q29a_name CASCADE;
DROP VIEW IF EXISTS imdb.q29a_role_type CASCADE;
DROP VIEW IF EXISTS imdb.q29a_title CASCADE;
DROP VIEW IF EXISTS imdb.q29a_person_info CASCADE;


CREATE VIEW imdb.q29a_comp_cast_type1 as
SELECT subject_id
FROM imdb.comp_cast_type as cct1
WHERE cct1.kind ='cast';

CREATE VIEW imdb.q29a_comp_cast_type2 as
SELECT subject_id as status_id
FROM imdb.comp_cast_type as cct2
WHERE cct2.kind ='complete+verified';

CREATE VIEW imdb.q29a_char_name as
SELECT person_role_id
FROM imdb.char_name as chn
WHERE chn.name = 'Queen';

CREATE VIEW imdb.q29a_cast_info as
SELECT person_id, movie_id, person_role_id, role_id
FROM imdb.cast_info as ci
WHERE ci.note IN ('(voice)',
                  '(voice) (uncredited)',
                  '(voice: English version)');

CREATE VIEW imdb.q29a_company_name as
SELECT company_id
FROM imdb.company_name as cn
WHERE cn.country_code ='[us]';

CREATE VIEW imdb.q29a_info_type1 as
SELECT info_type_id
FROM imdb.info_type as it1
WHERE it1.info = 'release dates';

CREATE VIEW imdb.q29a_info_type2 as
SELECT info_type_id as info_type_id2
FROM imdb.info_type as it2
WHERE it2.info = 'trivia';

CREATE VIEW imdb.q29a_keyword as
SELECT keyword_id
FROM imdb.keyword as k
WHERE k.keyword = 'computer-animation';

CREATE VIEW imdb.q29a_movie_info as
SELECT movie_id, info_type_id
FROM imdb.movie_info as mi
WHERE mi.info IS NOT NULL
  AND (mi.info LIKE 'Japan:%200%'
    OR mi.info LIKE 'USA:%200%');

CREATE VIEW imdb.q29a_name as
SELECT person_id
FROM imdb.name as n
WHERE n.gender ='f'
  AND n.name LIKE '%An%';

CREATE VIEW imdb.q29a_role_type as
SELECT role_id
FROM imdb.role_type as rt
WHERE rt.role ='actress';

CREATE VIEW imdb.q29a_title as
SELECT movie_id, kind_id
FROM imdb.title as t
WHERE t.title = 'Shrek 2'
  AND t.production_year BETWEEN 2000 AND 2010;

CREATE VIEW imdb.q29a_person_info as
SELECT person_id, info_type_id as info_type_id2
FROM imdb.person_info;

END;