-- implement the select predicates as views on top of the tables for JOB 8d
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/8d.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q8d_company_name CASCADE;
DROP VIEW IF EXISTS imdb.q8d_role_type CASCADE;

CREATE VIEW imdb.q8d_company_name as
SELECT company_id
FROM imdb.company_name as cn
WHERE cn.country_code ='[us]';

CREATE VIEW imdb.q8d_role_type as
SELECT role_id
FROM imdb.role_type as rt
WHERE rt.role ='costume designer';

END;
