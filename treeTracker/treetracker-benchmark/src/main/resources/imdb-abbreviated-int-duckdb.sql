-- setup Postgres database schemas of IMDB dataset for Join Ordering Benchmark (JOB)
-- This version we use int instead of varchar
-- This script, we rename the attributes to make them form natural join and drop redundant columns
-- instruction:
-- ./duckdb /home/zeyuanhu/projects/treetracker2/main.duckdb < /home/zeyuanhu/projects/treetracker2/treeTracker/treetracker-benchmark/src/main/resources/imdb-abbreviated-int-duckdb.sql
BEGIN TRANSACTION;

DROP SCHEMA IF EXISTS imdb_int CASCADE;
CREATE SCHEMA imdb_int;

CREATE TABLE imdb_int.aka_name_tmp (
                                                id varchar,
                                                person_id integer,
                                                name varchar,
                                                imdb_index varchar,
                                                name_pcode_cf varchar,
                                                name_pcode_nf varchar,
                                                surname_pcode varchar,
                                                md5sum varchar
);

CREATE TABLE imdb_int.aka_title_tmp (
                                                 id varchar,
                                                 movie_id integer,
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

CREATE TABLE imdb_int.cast_info_tmp (
                                                 id varchar,
                                                 person_id integer,
                                                 movie_id integer,
                                                 person_role_id integer,
                                                 note varchar,
                                                 nr_order varchar,
                                                 role_id integer
);

CREATE TABLE imdb_int.char_name_tmp (
                                                 person_role_id integer,
                                                 name varchar,
                                                 imdb_index varchar,
                                                 imdb_id varchar,
                                                 name_pcode_nf varchar,
                                                 surname_pcode varchar,
                                                 md5sum varchar
);

CREATE TABLE imdb_int.comp_cast_type_tmp (
                                                      subject_id integer,
                                                      kind varchar
);

CREATE TABLE imdb_int.company_name_tmp (
                                                    company_id integer,
                                                    name varchar,
                                                    country_code varchar,
                                                    imdb_id varchar,
                                                    name_pcode_nf varchar,
                                                    name_pcode_sf varchar,
                                                    md5sum varchar
);

CREATE TABLE imdb_int.company_type_tmp (
                                                    company_type_id integer,
                                                    kind varchar
);

CREATE TABLE imdb_int.complete_cast_tmp (
                                                     id varchar,
                                                     movie_id integer,
                                                     subject_id integer,
                                                     status_id integer
);

CREATE TABLE imdb_int.info_type_tmp (
                                                 info_type_id integer,
                                                 info varchar
);

CREATE TABLE imdb_int.keyword_tmp (
                                               keyword_id integer,
                                               keyword varchar,
                                               phonetic_code varchar
);

CREATE TABLE imdb_int.kind_type_tmp (
                                                 kind_id integer,
                                                 kind varchar
);

CREATE TABLE imdb_int.link_type_tmp (
                                                 link_type_id integer,
                                                 link varchar
);

CREATE TABLE imdb_int.movie_companies_tmp (
                                                       id varchar,
                                                       movie_id integer,
                                                       company_id integer,
                                                       company_type_id integer,
                                                       note varchar
);

CREATE TABLE imdb_int.movie_info_idx_tmp (
                                                      id varchar,
                                                      movie_id integer,
                                                      info_type_id integer,
                                                      info varchar,
                                                      note varchar
);

CREATE TABLE imdb_int.movie_keyword_tmp (
                                                     id varchar,
                                                     movie_id integer,
                                                     keyword_id integer
);

CREATE TABLE imdb_int.movie_link_tmp (
                                                  id varchar,
                                                  movie_id integer,
                                                  linked_movie_id integer,
                                                  link_type_id integer
);

CREATE TABLE imdb_int.name_tmp (
                                            person_id integer,
                                            name varchar,
                                            imdb_index varchar,
                                            imdb_id varchar,
                                            gender varchar,
                                            name_pcode_cf varchar,
                                            name_pcode_nf varchar,
                                            surname_pcode varchar,
                                            md5sum varchar
);

CREATE TABLE imdb_int.role_type_tmp (
                                                 role_id integer,
                                                 role varchar
);

CREATE TABLE imdb_int.title_tmp (
                                             movie_id integer,
                                             title varchar,
                                             imdb_index varchar,
                                             kind_id integer,
                                             production_year varchar,
                                             imdb_id varchar,
                                             phonetic_code varchar,
                                             episode_of_id varchar,
                                             season_nr varchar,
                                             episode_nr varchar,
                                             series_years varchar,
                                             md5sum varchar
);

CREATE TABLE imdb_int.movie_info_tmp (
                                                  id varchar,
                                                  movie_id integer,
                                                  info_type_id integer,
                                                  info varchar,
                                                  note varchar
);

CREATE TABLE imdb_int.person_info_tmp (
                                                   id varchar,
                                                   person_id integer,
                                                   info_type_id integer,
                                                   info varchar,
                                                   note varchar
);

COPY imdb_int.aka_name_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/aka_name.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb_int.aka_title_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/aka_title.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb_int.cast_info_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/cast_info.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb_int.char_name_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/char_name.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb_int.comp_cast_type_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/comp_cast_type.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb_int.company_name_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/company_name.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb_int.company_type_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/company_type.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb_int.complete_cast_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/complete_cast.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb_int.info_type_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/info_type.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb_int.keyword_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/keyword.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb_int.kind_type_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/kind_type.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb_int.link_type_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/link_type.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb_int.movie_companies_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/movie_companies.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb_int.movie_info_idx_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/movie_info_idx.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb_int.movie_keyword_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/movie_keyword.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb_int.movie_link_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/movie_link.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb_int.name_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/name.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb_int.role_type_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/role_type.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb_int.title_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/title.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb_int.movie_info_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/movie_info.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb_int.person_info_tmp FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/person_info.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;

CREATE TABLE imdb_int.aka_name AS SELECT person_id FROM imdb_int.aka_name_tmp;
CREATE TABLE imdb_int.aka_title AS SELECT movie_id FROM imdb_int.aka_title_tmp;
CREATE TABLE imdb_int.cast_info AS SELECT person_id, movie_id, person_role_id, role_id FROM imdb_int.cast_info_tmp;
CREATE TABLE imdb_int.char_name AS SELECT person_role_id FROM imdb_int.char_name_tmp;
CREATE TABLE imdb_int.comp_cast_type AS SELECT subject_id FROM imdb_int.comp_cast_type_tmp;
CREATE TABLE imdb_int.company_name AS SELECT company_id FROM imdb_int.company_name_tmp;
CREATE TABLE imdb_int.company_type AS SELECT company_type_id FROM imdb_int.company_type_tmp;
CREATE TABLE imdb_int.complete_cast AS SELECT movie_id, subject_id, status_id FROM imdb_int.complete_cast_tmp;
CREATE TABLE imdb_int.info_type AS SELECT info_type_id FROM imdb_int.info_type_tmp;
CREATE TABLE imdb_int.keyword AS SELECT keyword_id FROM imdb_int.keyword_tmp;
CREATE TABLE imdb_int.kind_type AS SELECT kind_id FROM imdb_int.kind_type_tmp;
CREATE TABLE imdb_int.link_type AS SELECT link_type_id FROM imdb_int.link_type_tmp;
CREATE TABLE imdb_int.movie_companies AS SELECT movie_id, company_id, company_type_id FROM imdb_int.movie_companies_tmp;
CREATE TABLE imdb_int.movie_info_idx AS SELECT movie_id, info_type_id  FROM imdb_int.movie_info_idx_tmp;
CREATE TABLE imdb_int.movie_keyword AS SELECT movie_id, keyword_id FROM imdb_int.movie_keyword_tmp;
CREATE TABLE imdb_int.movie_link AS SELECT movie_id, linked_movie_id, link_type_id FROM imdb_int.movie_link_tmp;
CREATE TABLE imdb_int.name AS SELECT person_id FROM imdb_int.name_tmp;
CREATE TABLE imdb_int.role_type AS SELECT role_id FROM imdb_int.role_type_tmp;
CREATE TABLE imdb_int.title AS SELECT movie_id, kind_id FROM imdb_int.title_tmp;
CREATE TABLE imdb_int.movie_info AS SELECT movie_id, info_type_id FROM imdb_int.movie_info_tmp;
CREATE TABLE imdb_int.person_info AS SELECT person_id, info_type_id FROM imdb_int.person_info_tmp;

DROP TABLE imdb_int.aka_name_tmp;
DROP TABLE imdb_int.aka_title_tmp;
DROP TABLE imdb_int.cast_info_tmp;
DROP TABLE imdb_int.char_name_tmp;
DROP TABLE imdb_int.comp_cast_type_tmp;
DROP TABLE imdb_int.company_name_tmp;
DROP TABLE imdb_int.company_type_tmp;
DROP TABLE imdb_int.complete_cast_tmp;
DROP TABLE imdb_int.info_type_tmp;
DROP TABLE imdb_int.keyword_tmp;
DROP TABLE imdb_int.kind_type_tmp;
DROP TABLE imdb_int.link_type_tmp;
DROP TABLE imdb_int.movie_companies_tmp;
DROP TABLE imdb_int.movie_info_idx_tmp;
DROP TABLE imdb_int.movie_keyword_tmp;
DROP TABLE imdb_int.movie_link_tmp;
DROP TABLE imdb_int.name_tmp;
DROP TABLE imdb_int.role_type_tmp;
DROP TABLE imdb_int.title_tmp;
DROP TABLE imdb_int.movie_info_tmp;
DROP TABLE imdb_int.person_info_tmp;

COMMIT;