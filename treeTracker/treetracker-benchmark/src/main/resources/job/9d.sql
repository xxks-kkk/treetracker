-- implement the select predicates as views on top of the tables for JOB 9d
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/9d.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q9d_cast_info CASCADE;
DROP VIEW IF EXISTS imdb.q9d_company_name CASCADE;
DROP VIEW IF EXISTS imdb.q9d_name CASCADE;
DROP VIEW IF EXISTS imdb.q9d_role_type CASCADE;


CREATE VIEW imdb.q9d_cast_info as
SELECT person_id, movie_id, person_role_id, role_id
FROM imdb.cast_info as ci
WHERE ci.note IN ('(voice)',
                  '(voice: Japanese version)',
                  '(voice) (uncredited)',
                  '(voice: English version)');

CREATE VIEW imdb.q9d_company_name as
SELECT company_id
FROM imdb.company_name as cn
WHERE cn.country_code ='[us]';

CREATE VIEW imdb.q9d_name as
SELECT person_id
FROM imdb.name as n
WHERE n.gender ='f';

CREATE VIEW imdb.q9d_role_type as
SELECT role_id
FROM imdb.role_type as rt
WHERE rt.role ='actress';


END;
