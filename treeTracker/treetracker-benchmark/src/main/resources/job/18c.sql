-- implement the select predicates as views on top of the tables for JOB 18c
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/18c.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q18c_cast_info CASCADE;
DROP VIEW IF EXISTS imdb.q18c_info_type1 CASCADE;
DROP VIEW IF EXISTS imdb.q18c_info_type2 CASCADE;
DROP VIEW IF EXISTS imdb.q18c_movie_info CASCADE;
DROP VIEW IF EXISTS imdb.q18c_movie_info_idx2 CASCADE;
DROP VIEW IF EXISTS imdb.q18c_name CASCADE;

CREATE VIEW imdb.q18c_cast_info as
SELECT person_id, movie_id, person_role_id, role_id
FROM imdb.cast_info as ci
WHERE ci.note IN ('(writer)',
                  '(head writer)',
                  '(written by)',
                  '(story)',
                  '(story editor)');

CREATE VIEW imdb.q18c_info_type1 as
SELECT info_type_id
FROM imdb.info_type as it1
WHERE it1.info = 'genres';

CREATE VIEW imdb.q18c_info_type2 as
SELECT info_type_id as info_type_id2
FROM imdb.info_type as it2
WHERE it2.info = 'votes';

CREATE VIEW imdb.q18c_movie_info as
SELECT movie_id, info_type_id
FROM imdb.movie_info as mi
WHERE mi.info IN ('Horror',
                  'Action',
                  'Sci-Fi',
                  'Thriller',
                  'Crime',
                  'War');

CREATE VIEW imdb.q18c_movie_info_idx2 as
SELECT movie_id, info_type_id as info_type_id2
FROM imdb.movie_info_idx as mi_idx;

CREATE VIEW imdb.q18c_name as
SELECT person_id
FROM imdb.name as n
WHERE n.gender = 'm';


END;