-- implement the select predicates as views on top of the tables for JOB 33a
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/33a.sql

/*
 Note on acyclicity:
 Details see "Possible sentence bug in Free Join paper" email. In digest,
 we perform renaming attributes to avoid unwanted join due to natural join if:
 1. there is no explicit join condition, e.g., mc1 and mc2 on company_type_id
 2. whenever there is an alis on the relation, the relation should be considered indepedently.
    For example, cn1 and cn2 are different relations and their attributes company_id should be
    renamed to different attributes. The only exception to this is there is an explicit join condition
    in the WHERE clause
 To confirm, we can use https://github.com/dmlongo/hgtools with, for example, the following arguments to the Main:
 -convert -sql /home/zeyuanhu/projects/job/join-order-benchmark/schema.sql /home/zeyuanhu/projects/challenge-set-gitlab/33a.sql
 (need to edit 33a.sql to get rid of unsupported where predicate). Then, checking the resulting 33a.hg and 33a.map.
 */
BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q33a_company_name1 CASCADE;
DROP VIEW IF EXISTS imdb.q33a_company_name2 CASCADE;
DROP VIEW IF EXISTS imdb.q33a_info_type1 CASCADE;
DROP VIEW IF EXISTS imdb.q33a_info_type2 CASCADE;
DROP VIEW IF EXISTS imdb.q33a_kind_type1 CASCADE;
DROP VIEW IF EXISTS imdb.q33a_kind_type2 CASCADE;
DROP VIEW IF EXISTS imdb.q33a_link_type CASCADE;
DROP VIEW IF EXISTS imdb.q33a_movie_info_idx1 CASCADE;
DROP VIEW IF EXISTS imdb.q33a_movie_info_idx2 CASCADE;
DROP VIEW IF EXISTS imdb.q33a_title1 CASCADE;
DROP VIEW IF EXISTS imdb.q33a_title2 CASCADE;
DROP VIEW IF EXISTS imdb.q33a_movie_companies1 CASCADE;
DROP VIEW IF EXISTS imdb.q33a_movie_companies2 CASCADE;

CREATE VIEW imdb.q33a_company_name1 as
SELECT company_id
FROM imdb.company_name as cn1
WHERE cn1.country_code = '[us]';

CREATE VIEW imdb.q33a_company_name2 as
SELECT company_id as company_id2
FROM imdb.company_name;

CREATE VIEW imdb.q33a_info_type1 as
SELECT info_type_id
FROM imdb.info_type as it1
WHERE it1.info = 'rating';

CREATE VIEW imdb.q33a_info_type2 as
SELECT info_type_id as info_type_id2
FROM imdb.info_type as it2
WHERE it2.info = 'rating';

CREATE VIEW imdb.q33a_kind_type1 as
SELECT kind_id
FROM imdb.kind_type as kt1
WHERE kt1.kind IN ('tv series');

CREATE VIEW imdb.q33a_kind_type2 as
SELECT kind_id as kind_id2
FROM imdb.kind_type as kt2
WHERE kt2.kind IN ('tv series');

CREATE VIEW imdb.q33a_link_type as
SELECT link_type_id
FROM imdb.link_type as lt
WHERE lt.link IN ('sequel',
                  'follows',
                  'followed by');

CREATE VIEW imdb.q33a_movie_info_idx1 as
SELECT movie_id, info_type_id
FROM imdb.movie_info_idx;

CREATE VIEW imdb.q33a_movie_info_idx2 as
SELECT movie_id as linked_movie_id, info_type_id as info_type_id2
FROM imdb.movie_info_idx as mi_idx2
WHERE mi_idx2.info < '3.0';

CREATE VIEW imdb.q33a_title1 as
SELECT movie_id, kind_id
FROM imdb.title;

CREATE VIEW imdb.q33a_title2 as
SELECT movie_id as linked_movie_id, kind_id as kind_id2
FROM imdb.title as t2
WHERE t2.production_year BETWEEN 2005 AND 2008;

CREATE VIEW imdb.q33a_movie_companies1 as
SELECT movie_id, company_id, company_type_id
FROM imdb.movie_companies;

/*
 Rename company_type_id due to it doesn't participate in the
 join condition.
 */
CREATE VIEW imdb.q33a_movie_companies2 as
SELECT movie_id as linked_movie_id, company_id as company_id2, company_type_id as company_type_id1
FROM imdb.movie_companies;

END;