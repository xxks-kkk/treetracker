-- implement the select predicates as views on top of the tables for JOB 30c
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/30c.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q30c_comp_cast_type1 CASCADE;
DROP VIEW IF EXISTS imdb.q30c_comp_cast_type2 CASCADE;
DROP VIEW IF EXISTS imdb.q30c_cast_info CASCADE;
DROP VIEW IF EXISTS imdb.q30c_info_type1 CASCADE;
DROP VIEW IF EXISTS imdb.q30c_info_type2 CASCADE;
DROP VIEW IF EXISTS imdb.q30c_keyword CASCADE;
DROP VIEW IF EXISTS imdb.q30c_movie_info CASCADE;
DROP VIEW IF EXISTS imdb.q30c_name CASCADE;
DROP VIEW IF EXISTS imdb.q30c_movie_info_idx2 CASCADE;

CREATE VIEW imdb.q30c_comp_cast_type1 as
SELECT subject_id
FROM imdb.comp_cast_type as cct1
WHERE cct1.kind = 'cast';

CREATE VIEW imdb.q30c_comp_cast_type2 as
SELECT subject_id as status_id
FROM imdb.comp_cast_type as cct2
WHERE cct2.kind ='complete+verified';

CREATE VIEW imdb.q30c_cast_info as
SELECT person_id, movie_id, person_role_id, role_id
FROM imdb.cast_info as ci
WHERE ci.note IN ('(writer)',
                  '(head writer)',
                  '(written by)',
                  '(story)',
                  '(story editor)');

CREATE VIEW imdb.q30c_info_type1 as
SELECT info_type_id
FROM imdb.info_type as it1
WHERE it1.info = 'genres';

CREATE VIEW imdb.q30c_info_type2 as
SELECT info_type_id as info_type_id2
FROM imdb.info_type as it2
WHERE it2.info = 'votes';

CREATE VIEW imdb.q30c_keyword as
SELECT keyword_id
FROM imdb.keyword as k
WHERE k.keyword IN ('murder',
                    'violence',
                    'blood',
                    'gore',
                    'death',
                    'female-nudity',
                    'hospital');

CREATE VIEW imdb.q30c_movie_info as
SELECT movie_id, info_type_id
FROM imdb.movie_info as mi
WHERE mi.info IN ('Horror',
                  'Action',
                  'Sci-Fi',
                  'Thriller',
                  'Crime',
                  'War');

CREATE VIEW imdb.q30c_name as
SELECT person_id
FROM imdb.name as n
WHERE n.gender = 'm';

CREATE VIEW imdb.q30c_movie_info_idx2 as
SELECT movie_id, info_type_id as info_type_id2
FROM imdb.movie_info_idx as mi_idx;

END;