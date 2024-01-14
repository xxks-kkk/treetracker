-- implement the select predicates as views on top of the tables for JOB 27a
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/27a.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q27a_comp_cast_type1 CASCADE;
DROP VIEW IF EXISTS imdb.q27a_comp_cast_type2 CASCADE;
DROP VIEW IF EXISTS imdb.q27a_company_name CASCADE;
DROP VIEW IF EXISTS imdb.q27a_company_type CASCADE;
DROP VIEW IF EXISTS imdb.q27a_keyword CASCADE;
DROP VIEW IF EXISTS imdb.q27a_link_type CASCADE;
DROP VIEW IF EXISTS imdb.q27a_movie_companies CASCADE;
DROP VIEW IF EXISTS imdb.q27a_movie_info CASCADE;
DROP VIEW IF EXISTS imdb.q27a_title CASCADE;


CREATE VIEW imdb.q27a_comp_cast_type1 as
SELECT subject_id
FROM imdb.comp_cast_type as cct1
WHERE cct1.kind IN ('cast',
                    'crew');

CREATE VIEW imdb.q27a_comp_cast_type2 as
SELECT subject_id as status_id
FROM imdb.comp_cast_type as cct2
WHERE cct2.kind = 'complete';

CREATE VIEW imdb.q27a_company_name as
SELECT company_id
FROM imdb.company_name as cn
WHERE cn.country_code !='[pl]'
  AND (cn.name LIKE '%Film%'
    OR cn.name LIKE '%Warner%');

CREATE VIEW imdb.q27a_company_type as
SELECT company_type_id
FROM imdb.company_type as ct
WHERE ct.kind ='production companies';

CREATE VIEW imdb.q27a_keyword as
SELECT keyword_id
FROM imdb.keyword as k
WHERE k.keyword ='sequel';

CREATE VIEW imdb.q27a_link_type as
SELECT link_type_id
FROM imdb.link_type as lt
WHERE lt.link LIKE '%follow%';

CREATE VIEW imdb.q27a_movie_companies as
SELECT movie_id, company_id, company_type_id
FROM imdb.movie_companies as mc
WHERE mc.note IS NULL;

CREATE VIEW imdb.q27a_movie_info as
SELECT movie_id, info_type_id
FROM imdb.movie_info as mi
WHERE mi.info IN ('Sweden',
                  'Germany',
                  'Swedish',
                  'German');

CREATE VIEW imdb.q27a_title as
SELECT movie_id, kind_id
FROM imdb.title as t
WHERE t.production_year BETWEEN 1950 AND 2000;

END;