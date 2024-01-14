-- implement the select predicates as views on top of the tables for JOB 7c
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/7c.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q7c_aka_name CASCADE;
DROP VIEW IF EXISTS imdb.q7c_info_type CASCADE;
DROP VIEW IF EXISTS imdb.q7c_link_type CASCADE;
DROP VIEW IF EXISTS imdb.q7c_name CASCADE;
DROP VIEW IF EXISTS imdb.q7c_person_info CASCADE;
DROP VIEW IF EXISTS imdb.q7c_title CASCADE;
DROP VIEW IF EXISTS imdb.q7c_movie_link CASCADE;

CREATE VIEW imdb.q7c_aka_name AS
SELECT person_id
FROM imdb.aka_name as an
WHERE an.name IS NOT NULL
  AND (an.name LIKE '%a%'
    OR an.name LIKE 'A%');

CREATE VIEW imdb.q7c_info_type as
SELECT info_type_id
FROM imdb.info_type as it
WHERE it.info = 'mini biography';

CREATE VIEW imdb.q7c_link_type as
SELECT link_type_id
FROM imdb.link_type as lt
WHERE lt.link IN ('references',
                  'referenced in',
                  'features',
                  'featured in');

CREATE VIEW imdb.q7c_name as
SELECT person_id
FROM imdb.name as n
WHERE n.name_pcode_cf BETWEEN 'A' AND 'F'
  AND (n.gender='m'
    OR (n.gender = 'f'
        AND n.name LIKE 'A%'));

CREATE VIEW imdb.q7c_person_info AS
SELECT person_id, info_type_id
FROM imdb.person_info as pi
WHERE pi.note IS NOT NULL;

CREATE VIEW imdb.q7c_title as
SELECT movie_id, kind_id
FROM imdb.title as t
WHERE t.production_year BETWEEN 1980 AND 2010;

CREATE VIEW imdb.q7c_movie_link as
SELECT linked_movie_id as movie_id, link_type_id
FROM imdb.movie_link;

END;
