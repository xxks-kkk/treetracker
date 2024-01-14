-- implement the select predicates as views on top of the tables for JOB 30a
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/30a.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q30a_comp_cast_type1 CASCADE;
DROP VIEW IF EXISTS imdb.q30a_comp_cast_type2 CASCADE;
DROP VIEW IF EXISTS imdb.q30a_cast_info CASCADE;
DROP VIEW IF EXISTS imdb.q30a_info_type1 CASCADE;
DROP VIEW IF EXISTS imdb.q30a_info_type2 CASCADE;
DROP VIEW IF EXISTS imdb.q30a_keyword CASCADE;
DROP VIEW IF EXISTS imdb.q30a_movie_info CASCADE;
DROP VIEW IF EXISTS imdb.q30a_name CASCADE;
DROP VIEW IF EXISTS imdb.q30a_title CASCADE;
DROP VIEW IF EXISTS imdb.q30a_movie_info_idx2 CASCADE;


CREATE VIEW imdb.q30a_comp_cast_type1 as
SELECT subject_id
FROM imdb.comp_cast_type as cct1
WHERE cct1.kind IN ('cast',
                    'crew');

CREATE VIEW imdb.q30a_comp_cast_type2 as
SELECT subject_id as status_id
FROM imdb.comp_cast_type as cct2
WHERE cct2.kind ='complete+verified';

CREATE VIEW imdb.q30a_cast_info as
SELECT person_id, movie_id, person_role_id, role_id
FROM imdb.cast_info as ci
WHERE ci.note IN ('(writer)',
                  '(head writer)',
                  '(written by)',
                  '(story)',
                  '(story editor)');

CREATE VIEW imdb.q30a_info_type1 as
SELECT info_type_id
FROM imdb.info_type as it1
WHERE it1.info = 'genres';

CREATE VIEW imdb.q30a_info_type2 as
SELECT info_type_id as info_type_id2
FROM imdb.info_type as it2
WHERE it2.info = 'votes';

CREATE VIEW imdb.q30a_keyword as
SELECT keyword_id
FROM imdb.keyword as k
WHERE k.keyword IN ('murder',
                    'violence',
                    'blood',
                    'gore',
                    'death',
                    'female-nudity',
                    'hospital');

CREATE VIEW imdb.q30a_movie_info as
SELECT movie_id, info_type_id
FROM imdb.movie_info as mi
WHERE mi.info IN ('Horror',
                  'Thriller');

CREATE VIEW imdb.q30a_name as
SELECT person_id
FROM imdb.name as n
WHERE n.gender = 'm';

CREATE VIEW imdb.q30a_title as
SELECT movie_id, kind_id
FROM imdb.title as t
WHERE t.production_year > 2000;

CREATE VIEW imdb.q30a_movie_info_idx2 as
SELECT movie_id, info_type_id as info_type_id2
FROM imdb.movie_info_idx as mi_idx;

END;