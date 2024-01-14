-- setup Postgres database schemas of IMDB dataset for Join Ordering Benchmark (JOB)
-- This script, we rename the attributes to make them form natural join and drop redundant columns
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/imdb.sql
BEGIN TRANSACTION;

DROP SCHEMA IF EXISTS imdb CASCADE;
CREATE SCHEMA imdb;

CREATE UNLOGGED TABLE imdb.aka_name_tmp (
                          id varchar,
                          person_id varchar,
                          name varchar,
                          imdb_index varchar,
                          name_pcode_cf varchar,
                          name_pcode_nf varchar,
                          surname_pcode varchar,
                          md5sum varchar
);

CREATE UNLOGGED TABLE imdb.aka_title_tmp (
                           id varchar,
                           movie_id varchar,
                           title varchar,
                           imdb_index varchar,
                           kind_id varchar,
                           production_year varchar,
                           phonetic_code varchar,
                           episode_of_id varchar,
                           season_nr varchar,
                           episode_nr varchar,
                           note varchar,
                           md5sum varchar
);

CREATE UNLOGGED TABLE imdb.cast_info_tmp (
                           id varchar,
                           person_id varchar,
                           movie_id varchar,
                           person_role_id varchar,
                           note varchar,
                           nr_order varchar,
                           role_id varchar
);

CREATE UNLOGGED TABLE imdb.char_name_tmp (
                           person_role_id varchar,
                           name varchar,
                           imdb_index varchar,
                           imdb_id varchar,
                           name_pcode_nf varchar,
                           surname_pcode varchar,
                           md5sum varchar
);

CREATE UNLOGGED TABLE imdb.comp_cast_type_tmp (
                                subject_id varchar,
                                kind varchar
);

CREATE UNLOGGED TABLE imdb.company_name_tmp (
                              company_id varchar,
                              name varchar,
                              country_code varchar,
                              imdb_id varchar,
                              name_pcode_nf varchar,
                              name_pcode_sf varchar,
                              md5sum varchar
);

CREATE UNLOGGED TABLE imdb.company_type_tmp (
                              company_type_id varchar,
                              kind varchar
);

CREATE UNLOGGED TABLE imdb.complete_cast_tmp (
                               id varchar,
                               movie_id varchar,
                               subject_id varchar,
                               status_id varchar
);

CREATE UNLOGGED TABLE imdb.info_type_tmp (
                           info_type_id varchar,
                           info varchar
);

CREATE UNLOGGED TABLE imdb.keyword_tmp (
                         keyword_id varchar,
                         keyword varchar,
                         phonetic_code varchar
);

CREATE UNLOGGED TABLE imdb.kind_type_tmp (
                           kind_id varchar,
                           kind varchar
);

CREATE UNLOGGED TABLE imdb.link_type_tmp (
                           link_type_id varchar,
                           link varchar
);

CREATE UNLOGGED TABLE imdb.movie_companies_tmp (
                                 id varchar,
                                 movie_id varchar,
                                 company_id varchar,
                                 company_type_id varchar,
                                 note varchar
);

CREATE UNLOGGED TABLE imdb.movie_info_idx_tmp (
                                id varchar,
                                movie_id varchar,
                                info_type_id varchar,
                                info varchar,
                                note varchar
);

CREATE UNLOGGED TABLE imdb.movie_keyword_tmp (
                               id varchar,
                               movie_id varchar,
                               keyword_id varchar
);

CREATE UNLOGGED TABLE imdb.movie_link_tmp (
                            id varchar,
                            movie_id varchar,
                            linked_movie_id varchar,
                            link_type_id varchar
);

CREATE UNLOGGED TABLE imdb.name_tmp (
                      person_id varchar,
                      name varchar,
                      imdb_index varchar,
                      imdb_id varchar,
                      gender varchar,
                      name_pcode_cf varchar,
                      name_pcode_nf varchar,
                      surname_pcode varchar,
                      md5sum varchar
);

CREATE UNLOGGED TABLE imdb.role_type_tmp (
                           role_id varchar,
                           role varchar
);

CREATE UNLOGGED TABLE imdb.title_tmp (
                       movie_id varchar,
                       title varchar,
                       imdb_index varchar,
                       kind_id varchar,
                       production_year varchar,
                       imdb_id varchar,
                       phonetic_code varchar,
                       episode_of_id varchar,
                       season_nr varchar,
                       episode_nr varchar,
                       series_years varchar,
                       md5sum varchar
);

CREATE UNLOGGED TABLE imdb.movie_info_tmp (
                            id varchar,
                            movie_id varchar,
                            info_type_id varchar,
                            info varchar,
                            note varchar
);

CREATE UNLOGGED TABLE imdb.person_info_tmp (
                             id varchar,
                             person_id varchar,
                             info_type_id varchar,
                             info varchar,
                             note varchar
);

COPY imdb.aka_name_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/aka_name.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb.aka_title_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/aka_title.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb.cast_info_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/cast_info.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb.char_name_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/char_name.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb.comp_cast_type_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/comp_cast_type.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb.company_name_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/company_name.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb.company_type_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/company_type.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb.complete_cast_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/complete_cast.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb.info_type_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/info_type.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb.keyword_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/keyword.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb.kind_type_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/kind_type.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb.link_type_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/link_type.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb.movie_companies_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/movie_companies.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb.movie_info_idx_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/movie_info_idx.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb.movie_keyword_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/movie_keyword.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb.movie_link_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/movie_link.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb.name_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/name.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb.role_type_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/role_type.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb.title_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/title.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb.movie_info_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/movie_info.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb.person_info_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/person_info.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;

SELECT person_id INTO imdb.aka_name FROM imdb.aka_name_tmp;
SELECT movie_id INTO imdb.aka_title FROM imdb.aka_title_tmp;
SELECT person_id, movie_id, person_role_id, role_id INTO imdb.cast_info FROM imdb.cast_info_tmp;
SELECT person_role_id INTO imdb.char_name FROM imdb.char_name_tmp;
SELECT subject_id INTO imdb.comp_cast_type FROM imdb.comp_cast_type_tmp;
SELECT company_id INTO imdb.company_name FROM imdb.company_name_tmp;
SELECT company_type_id INTO imdb.company_type FROM imdb.company_type_tmp;
SELECT movie_id, subject_id, status_id INTO imdb.complete_cast FROM imdb.complete_cast_tmp;
SELECT info_type_id INTO imdb.info_type FROM imdb.info_type_tmp;
SELECT keyword_id INTO imdb.keyword FROM imdb.keyword_tmp;
SELECT kind_id INTO imdb.kind_type FROM imdb.kind_type_tmp;
SELECT link_type_id INTO imdb.link_type FROM imdb.link_type_tmp;
SELECT movie_id, company_id, company_type_id INTO imdb.movie_companies FROM imdb.movie_companies_tmp;
SELECT movie_id, info_type_id INTO imdb.movie_info_idx FROM imdb.movie_info_idx_tmp;
SELECT movie_id, keyword_id INTO imdb.movie_keyword FROM imdb.movie_keyword_tmp;
SELECT movie_id, linked_movie_id, link_type_id INTO imdb.movie_link FROM imdb.movie_link_tmp;
SELECT person_id INTO imdb.name FROM imdb.name_tmp;
SELECT role_id INTO imdb.role_type FROM imdb.role_type_tmp;
SELECT movie_id, kind_id INTO imdb.title FROM imdb.title_tmp;
SELECT movie_id, info_type_id INTO imdb.movie_info FROM imdb.movie_info_tmp;
SELECT person_id, info_type_id INTO imdb.person_info FROM imdb.person_info_tmp;

DROP TABLE imdb.aka_name_tmp;
DROP TABLE imdb.aka_title_tmp;
DROP TABLE imdb.cast_info_tmp;
DROP TABLE imdb.char_name_tmp;
DROP TABLE imdb.comp_cast_type_tmp;
DROP TABLE imdb.company_name_tmp;
DROP TABLE imdb.company_type_tmp;
DROP TABLE imdb.complete_cast_tmp;
DROP TABLE imdb.info_type_tmp;
DROP TABLE imdb.keyword_tmp;
DROP TABLE imdb.kind_type_tmp;
DROP TABLE imdb.link_type_tmp;
DROP TABLE imdb.movie_companies_tmp;
DROP TABLE imdb.movie_info_idx_tmp;
DROP TABLE imdb.movie_keyword_tmp;
DROP TABLE imdb.movie_link_tmp;
DROP TABLE imdb.name_tmp;
DROP TABLE imdb.role_type_tmp;
DROP TABLE imdb.title_tmp;
DROP TABLE imdb.movie_info_tmp;
DROP TABLE imdb.person_info_tmp;

COMMIT;

ALTER TABLE imdb.aka_name SET LOGGED;
ALTER TABLE imdb.aka_title SET LOGGED;
ALTER TABLE imdb.cast_info SET LOGGED;
ALTER TABLE imdb.char_name SET LOGGED;
ALTER TABLE imdb.comp_cast_type SET LOGGED;
ALTER TABLE imdb.company_name SET LOGGED;
ALTER TABLE imdb.company_type SET LOGGED;
ALTER TABLE imdb.complete_cast SET LOGGED;
ALTER TABLE imdb.info_type SET LOGGED;
ALTER TABLE imdb.keyword SET LOGGED;
ALTER TABLE imdb.kind_type SET LOGGED;
ALTER TABLE imdb.link_type SET LOGGED;
ALTER TABLE imdb.movie_companies SET LOGGED;
ALTER TABLE imdb.movie_info_idx SET LOGGED;
ALTER TABLE imdb.movie_keyword SET LOGGED;
ALTER TABLE imdb.movie_link SET LOGGED;
ALTER TABLE imdb.name SET LOGGED;
ALTER TABLE imdb.role_type SET LOGGED;
ALTER TABLE imdb.title SET LOGGED;
ALTER TABLE imdb.movie_info SET LOGGED;
ALTER TABLE imdb.person_info SET LOGGED;