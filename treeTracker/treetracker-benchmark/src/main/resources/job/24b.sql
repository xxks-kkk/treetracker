-- implement the select predicates as views on top of the tables for JOB 24b
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/24b.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q24b_cast_info CASCADE;
DROP VIEW IF EXISTS imdb.q24b_company_name CASCADE;
DROP VIEW IF EXISTS imdb.q24b_info_type CASCADE;
DROP VIEW IF EXISTS imdb.q24b_keyword CASCADE;
DROP VIEW IF EXISTS imdb.q24b_movie_info CASCADE;
DROP VIEW IF EXISTS imdb.q24b_name CASCADE;
DROP VIEW IF EXISTS imdb.q24b_role_type CASCADE;
DROP VIEW IF EXISTS imdb.q24b_title CASCADE;


CREATE VIEW imdb.q24b_cast_info as
SELECT person_id, movie_id, person_role_id, role_id
FROM imdb.cast_info as ci
WHERE ci.note IN ('(voice)',
                  '(voice: Japanese version)',
                  '(voice) (uncredited)',
                  '(voice: English version)');

CREATE VIEW imdb.q24b_company_name as
SELECT company_id
FROM imdb.company_name as cn
WHERE cn.country_code ='[us]'
  AND cn.name = 'DreamWorks Animation';

CREATE VIEW imdb.q24b_info_type as
SELECT info_type_id
FROM imdb.info_type as it
WHERE it.info = 'release dates';

CREATE VIEW imdb.q24b_keyword as
SELECT keyword_id
FROM imdb.keyword as k
WHERE k.keyword IN ('hero',
                    'martial-arts',
                    'hand-to-hand-combat',
                    'computer-animated-movie');

CREATE VIEW imdb.q24b_movie_info as
SELECT movie_id, info_type_id
FROM imdb.movie_info as mi
WHERE mi.info IS NOT NULL
  AND (mi.info LIKE 'Japan:%201%'
    OR mi.info LIKE 'USA:%201%');

CREATE VIEW imdb.q24b_name as
SELECT person_id
FROM imdb.name as n
WHERE n.gender ='f'
  AND n.name LIKE '%An%';

CREATE VIEW imdb.q24b_role_type as
SELECT role_id
FROM imdb.role_type as rt
WHERE rt.role = 'actress';

CREATE VIEW imdb.q24b_title as
SELECT movie_id, kind_id
FROM imdb.title as t
WHERE t.production_year > 2010
  AND t.title LIKE 'Kung Fu Panda%';

END;