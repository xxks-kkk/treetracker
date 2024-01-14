-- implement the select predicates as views on top of the tables for JOB 14b
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/14b.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q14b_info_type1 CASCADE;
DROP VIEW IF EXISTS imdb.q14b_info_type2 CASCADE;
DROP VIEW IF EXISTS imdb.q14b_keyword CASCADE;
DROP VIEW IF EXISTS imdb.q14b_kind_type CASCADE;
DROP VIEW IF EXISTS imdb.q14b_movie_info CASCADE;
DROP VIEW IF EXISTS imdb.q14b_movie_info_idx2 CASCADE;
DROP VIEW IF EXISTS imdb.q14b_title CASCADE;

CREATE VIEW imdb.q14b_info_type1 as
SELECT info_type_id
FROM imdb.info_type as it1
WHERE it1.info = 'countries';

CREATE VIEW imdb.q14b_info_type2 as
SELECT info_type_id as info_type_id2
FROM imdb.info_type as it2
WHERE it2.info = 'rating';

CREATE VIEW imdb.q14b_keyword as
SELECT keyword_id
FROM imdb.keyword as k
WHERE k.keyword IN ('murder',
                    'murder-in-title');

CREATE VIEW imdb.q14b_kind_type as
SELECT kind_id
FROM imdb.kind_type as kt
WHERE kt.kind = 'movie';

CREATE VIEW imdb.q14b_movie_info as
SELECT movie_id, info_type_id
FROM imdb.movie_info as mi
WHERE mi.info IN ('Sweden',
                  'Norway',
                  'Germany',
                  'Denmark',
                  'Swedish',
                  'Denish',
                  'Norwegian',
                  'German',
                  'USA',
                  'American');

CREATE VIEW imdb.q14b_movie_info_idx2 as
SELECT movie_id, info_type_id as info_type_id2
FROM imdb.movie_info_idx as mi_idx
WHERE mi_idx.info > '6.0';

CREATE VIEW imdb.q14b_title as
SELECT movie_id, kind_id
FROM imdb.title as t
WHERE t.production_year > 2010
  AND (t.title LIKE '%murder%'
    OR t.title LIKE '%Murder%'
    OR t.title LIKE '%Mord%');


END;