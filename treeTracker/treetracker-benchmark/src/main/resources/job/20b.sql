-- implement the select predicates as views on top of the tables for JOB 20b
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/20b.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q20b_comp_cast_type1 CASCADE;
DROP VIEW IF EXISTS imdb.q20b_comp_cast_type2 CASCADE;
DROP VIEW IF EXISTS imdb.q20b_char_name CASCADE;
DROP VIEW IF EXISTS imdb.q20b_keyword CASCADE;
DROP VIEW IF EXISTS imdb.q20b_kind_type CASCADE;
DROP VIEW IF EXISTS imdb.q20b_name CASCADE;
DROP VIEW IF EXISTS imdb.q20b_title CASCADE;


CREATE VIEW imdb.q20b_comp_cast_type1 as
SELECT subject_id
FROM imdb.comp_cast_type as cct1
WHERE cct1.kind = 'cast';

CREATE VIEW imdb.q20b_comp_cast_type2 as
SELECT subject_id as status_id
FROM imdb.comp_cast_type as cct2
WHERE cct2.kind LIKE '%complete%';

CREATE VIEW imdb.q20b_char_name as
SELECT person_role_id
FROM imdb.char_name as chn
WHERE chn.name NOT LIKE '%Sherlock%'
  AND (chn.name LIKE '%Tony%Stark%'
    OR chn.name LIKE '%Iron%Man%');

CREATE VIEW imdb.q20b_keyword as
SELECT keyword_id
FROM imdb.keyword as k
WHERE k.keyword IN ('superhero',
                    'sequel',
                    'second-part',
                    'marvel-comics',
                    'based-on-comic',
                    'tv-special',
                    'fight',
                    'violence');

CREATE VIEW imdb.q20b_kind_type as
SELECT kind_id
FROM imdb.kind_type as kt
WHERE kt.kind = 'movie';

CREATE VIEW imdb.q20b_name as
SELECT person_id
FROM imdb.name as n
WHERE n.name LIKE '%Downey%Robert%';

CREATE VIEW imdb.q20b_title as
SELECT movie_id, kind_id
FROM imdb.title as t
WHERE t.production_year > 2000;

END;