-- implement the select predicates as views on top of the tables for JOB 9a
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/9a.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q9a_cast_info CASCADE;
DROP VIEW IF EXISTS imdb.q9a_company_name CASCADE;
DROP VIEW IF EXISTS imdb.q9a_movie_companies CASCADE;
DROP VIEW IF EXISTS imdb.q9a_name CASCADE;
DROP VIEW IF EXISTS imdb.q9a_role_type CASCADE;
DROP VIEW IF EXISTS imdb.q9a_title CASCADE;


CREATE VIEW imdb.q9a_cast_info as
SELECT person_id, movie_id, person_role_id, role_id
FROM imdb.cast_info as ci
WHERE ci.note IN ('(voice)',
                  '(voice: Japanese version)',
                  '(voice) (uncredited)',
                  '(voice: English version)');

CREATE VIEW imdb.q9a_company_name as
SELECT company_id
FROM imdb.company_name as cn
WHERE cn.country_code ='[us]';

CREATE VIEW imdb.q9a_movie_companies as
SELECT movie_id, company_id, company_type_id
FROM imdb.movie_companies as mc
WHERE  mc.note IS NOT NULL
  AND (mc.note LIKE '%(USA)%'
    OR mc.note LIKE '%(worldwide)%');

CREATE VIEW imdb.q9a_name as
SELECT person_id
FROM imdb.name as n
WHERE n.gender ='f'
  AND n.name LIKE '%Ang%';

CREATE VIEW imdb.q9a_role_type as
SELECT role_id
FROM imdb.role_type as rt
WHERE rt.role ='actress';

CREATE VIEW imdb.q9a_title as
SELECT movie_id, kind_id
FROM imdb.title as t
WHERE t.production_year BETWEEN 2005 AND 2015;


END;
