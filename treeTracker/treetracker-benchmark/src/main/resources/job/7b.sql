-- implement the select predicates as views on top of the tables for JOB 7b
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/7b.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q7b_aka_name CASCADE;
DROP VIEW IF EXISTS imdb.q7b_info_type CASCADE;
DROP VIEW IF EXISTS imdb.q7b_link_type CASCADE;
DROP VIEW IF EXISTS imdb.q7b_name CASCADE;
DROP VIEW IF EXISTS imdb.q7b_person_info CASCADE;
DROP VIEW IF EXISTS imdb.q7b_title CASCADE;
DROP VIEW IF EXISTS imdb.q7b_movie_link CASCADE;

CREATE VIEW imdb.q7b_aka_name AS
SELECT person_id
FROM imdb.aka_name as an
WHERE an.name LIKE '%a%';

CREATE VIEW imdb.q7b_info_type as
SELECT info_type_id
FROM imdb.info_type as it
WHERE it.info ='mini biography';

CREATE VIEW imdb.q7b_link_type as
SELECT link_type_id
FROM imdb.link_type as lt
WHERE lt.link ='features';

CREATE VIEW imdb.q7b_name as
SELECT person_id
FROM imdb.name as n
WHERE n.name_pcode_cf LIKE 'D%'
  AND n.gender='m';

CREATE VIEW imdb.q7b_person_info AS
SELECT person_id, info_type_id
FROM imdb.person_info as pi
WHERE pi.note ='Volker Boehm';

CREATE VIEW imdb.q7b_title as
SELECT movie_id, kind_id
FROM imdb.title as t
WHERE t.production_year BETWEEN 1980 AND 1984;

CREATE VIEW imdb.q7b_movie_link as
SELECT linked_movie_id as movie_id, link_type_id
FROM imdb.movie_link;

END;
