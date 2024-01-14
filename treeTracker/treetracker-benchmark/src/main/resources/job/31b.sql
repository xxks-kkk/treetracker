-- implement the select predicates as views on top of the tables for JOB 31b
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/31b.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q31b_cast_info CASCADE;
DROP VIEW IF EXISTS imdb.q31b_company_name CASCADE;
DROP VIEW IF EXISTS imdb.q31b_info_type1 CASCADE;
DROP VIEW IF EXISTS imdb.q31b_info_type2 CASCADE;
DROP VIEW IF EXISTS imdb.q31b_keyword CASCADE;
DROP VIEW IF EXISTS imdb.q31b_movie_companies CASCADE;
DROP VIEW IF EXISTS imdb.q31b_movie_info CASCADE;
DROP VIEW IF EXISTS imdb.q31b_name CASCADE;
DROP VIEW IF EXISTS imdb.q31b_title CASCADE;
DROP VIEW IF EXISTS imdb.q31b_movie_info_idx2 CASCADE;


CREATE VIEW imdb.q31b_cast_info as
SELECT person_id, movie_id, person_role_id, role_id
FROM imdb.cast_info as ci
WHERE ci.note IN ('(writer)',
                  '(head writer)',
                  '(written by)',
                  '(story)',
                  '(story editor)');

CREATE VIEW imdb.q31b_company_name as
SELECT company_id
FROM imdb.company_name as cn
WHERE cn.name LIKE 'Lionsgate%';

CREATE VIEW imdb.q31b_info_type1 as
SELECT info_type_id
FROM imdb.info_type as it1
WHERE it1.info = 'genres';

CREATE VIEW imdb.q31b_info_type2 as
SELECT info_type_id as info_type_id2
FROM imdb.info_type as it2
WHERE it2.info = 'votes';

CREATE VIEW imdb.q31b_keyword as
SELECT keyword_id
FROM imdb.keyword as k
WHERE k.keyword IN ('murder',
                    'violence',
                    'blood',
                    'gore',
                    'death',
                    'female-nudity',
                    'hospital');

CREATE VIEW imdb.q31b_movie_companies as
SELECT movie_id, company_id, company_type_id
FROM imdb.movie_companies as mc
WHERE mc.note LIKE '%(Blu-ray)%';

CREATE VIEW imdb.q31b_movie_info as
SELECT movie_id, info_type_id
FROM imdb.movie_info as mi
WHERE mi.info IN ('Horror',
                  'Thriller');

CREATE VIEW imdb.q31b_name as
SELECT person_id
FROM imdb.name as n
WHERE n.gender = 'm';

CREATE VIEW imdb.q31b_title as
SELECT movie_id, kind_id
FROM imdb.title as t
WHERE t.production_year > 2000
  AND (t.title LIKE '%Freddy%'
    OR t.title LIKE '%Jason%'
    OR t.title LIKE 'Saw%');

CREATE VIEW imdb.q31b_movie_info_idx2 as
SELECT movie_id, info_type_id as info_type_id2
FROM imdb.movie_info_idx as mi_idx;

END;