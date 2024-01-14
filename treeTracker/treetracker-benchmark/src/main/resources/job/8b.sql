-- implement the select predicates as views on top of the tables for JOB 8b
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/8b.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q8b_cast_info CASCADE;
DROP VIEW IF EXISTS imdb.q8b_company_name CASCADE;
DROP VIEW IF EXISTS imdb.q8b_movie_companies CASCADE;
DROP VIEW IF EXISTS imdb.q8b_name CASCADE;
DROP VIEW IF EXISTS imdb.q8b_role_type CASCADE;
DROP VIEW IF EXISTS imdb.q8b_title CASCADE;

CREATE VIEW imdb.q8b_cast_info as
SELECT person_id, movie_id, person_role_id, role_id
FROM imdb.cast_info as ci
WHERE ci.note ='(voice: English version)';

CREATE VIEW imdb.q8b_company_name as
SELECT company_id
FROM imdb.company_name as cn
WHERE cn.country_code ='[jp]';

CREATE VIEW imdb.q8b_movie_companies as
SELECT movie_id, company_id, company_type_id
FROM imdb.movie_companies as mc
WHERE mc.note LIKE '%(Japan)%'
  AND mc.note NOT LIKE '%(USA)%'
  AND (mc.note LIKE '%(2006)%'
    OR mc.note LIKE '%(2007)%');

CREATE VIEW imdb.q8b_name as
SELECT person_id
FROM imdb.name as n
WHERE n.name LIKE '%Yo%'
  AND n.name NOT LIKE '%Yu%';

CREATE VIEW imdb.q8b_role_type as
SELECT role_id
FROM imdb.role_type as rt
WHERE rt.role ='actress';

CREATE VIEW imdb.q8b_title as
SELECT movie_id, kind_id
FROM imdb.title as t
WHERE t.production_year BETWEEN 2006 AND 2007
  AND (t.title LIKE 'One Piece%'
    OR t.title LIKE 'Dragon Ball Z%');

END;
