-- setup Postgres database schemas of IMDB dataset for Join Ordering Benchmark (JOB)
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/imdb.sql
BEGIN TRANSACTION;

DROP SCHEMA IF EXISTS imdb CASCADE;
CREATE SCHEMA imdb;

CREATE UNLOGGED TABLE imdb.aka_name (
                                   id integer NOT NULL,
                                   person_id integer NOT NULL,
                                   name text NOT NULL,
                                   imdb_index character varying(12),
                                   name_pcode_cf character varying(5),
                                   name_pcode_nf character varying(5),
                                   surname_pcode character varying(5),
                                   md5sum character varying(32)
);

CREATE UNLOGGED TABLE imdb.aka_title (
                                    id integer NOT NULL,
                                    movie_id integer NOT NULL,
                                    title text NOT NULL,
                                    imdb_index character varying(12),
                                    kind_id integer NOT NULL,
                                    production_year integer,
                                    phonetic_code character varying(5),
                                    episode_of_id integer,
                                    season_nr integer,
                                    episode_nr integer,
                                    note text,
                                    md5sum character varying(32)
);

CREATE UNLOGGED TABLE imdb.cast_info (
                                    id integer NOT NULL,
                                    person_id integer NOT NULL,
                                    movie_id integer NOT NULL,
                                    person_role_id integer,
                                    note text,
                                    nr_order integer,
                                    role_id integer NOT NULL
);

CREATE UNLOGGED TABLE imdb.char_name (
                                    person_role_id integer NOT NULL,
                                    name text NOT NULL,
                                    imdb_index character varying(12),
                                    imdb_id integer,
                                    name_pcode_nf character varying(5),
                                    surname_pcode character varying(5),
                                    md5sum character varying(32)
);

CREATE UNLOGGED TABLE imdb.comp_cast_type (
                                         subject_id integer NOT NULL,
                                         kind character varying(32) NOT NULL
);

CREATE UNLOGGED TABLE imdb.company_name (
                                       company_id integer NOT NULL,
                                       name text NOT NULL,
                                       country_code character varying(255),
                                       imdb_id integer,
                                       name_pcode_nf character varying(5),
                                       name_pcode_sf character varying(5),
                                       md5sum character varying(32)
);

CREATE UNLOGGED TABLE imdb.company_type (
                                       company_type_id integer NOT NULL,
                                       kind character varying(32) NOT NULL
);

CREATE UNLOGGED TABLE imdb.complete_cast (
                                        id integer NOT NULL,
                                        movie_id integer NOT NULL,
                                        subject_id integer NOT NULL,
                                        status_id integer NOT NULL
);

CREATE UNLOGGED TABLE imdb.info_type (
                                    info_type_id integer NOT NULL,
                                    info character varying(32) NOT NULL
);

CREATE UNLOGGED TABLE imdb.keyword (
                                  keyword_id integer NOT NULL,
                                  keyword text NOT NULL,
                                  phonetic_code character varying(5)
);

CREATE UNLOGGED TABLE imdb.kind_type (
                                    kind_id integer NOT NULL,
                                    kind character varying(15) NOT NULL
);

CREATE UNLOGGED TABLE imdb.link_type (
                                    link_type_id integer NOT NULL,
                                    link character varying(32) NOT NULL
);

CREATE UNLOGGED TABLE imdb.movie_companies (
                                          id integer NOT NULL,
                                          movie_id integer NOT NULL,
                                          company_id integer NOT NULL,
                                          company_type_id integer NOT NULL,
                                          note text
);

CREATE UNLOGGED TABLE imdb.movie_info (
                                     id integer NOT NULL,
                                     movie_id integer NOT NULL,
                                     info_type_id integer NOT NULL,
                                     info text NOT NULL,
                                     note text
);

CREATE UNLOGGED TABLE imdb.movie_info_idx (
                                         id integer NOT NULL,
                                         movie_id integer NOT NULL,
                                         info_type_id integer NOT NULL,
                                         info text NOT NULL,
                                         note text
);

CREATE UNLOGGED TABLE imdb.movie_keyword (
                                        id integer NOT NULL,
                                        movie_id integer NOT NULL,
                                        keyword_id integer NOT NULL
);

CREATE UNLOGGED TABLE imdb.movie_link (
                                     id integer NOT NULL,
                                     movie_id integer NOT NULL,
                                     linked_movie_id integer NOT NULL,
                                     link_type_id integer NOT NULL
);

CREATE UNLOGGED TABLE imdb.name (
                               person_id integer NOT NULL,
                               name text NOT NULL,
                               imdb_index character varying(12),
                               imdb_id integer,
                               gender character varying(1),
                               name_pcode_cf character varying(5),
                               name_pcode_nf character varying(5),
                               surname_pcode character varying(5),
                               md5sum character varying(32)
);

CREATE UNLOGGED TABLE imdb.person_info (
                                      id integer NOT NULL,
                                      person_id integer NOT NULL,
                                      info_type_id integer NOT NULL,
                                      info text NOT NULL,
                                      note text
);

CREATE UNLOGGED TABLE imdb.role_type (
                                    role_id integer NOT NULL,
                                    role character varying(32) NOT NULL
);

CREATE UNLOGGED TABLE imdb.title (
                                movie_id integer NOT NULL,
                                title text NOT NULL,
                                imdb_index character varying(12),
                                kind_id integer NOT NULL,
                                production_year integer,
                                imdb_id integer,
                                phonetic_code character varying(5),
                                episode_of_id integer,
                                season_nr integer,
                                episode_nr integer,
                                series_years character varying(49),
                                md5sum character varying(32)
);

COPY imdb.aka_name FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/aka_name.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb.aka_title FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/aka_title.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb.cast_info FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/cast_info.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb.char_name FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/char_name.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb.comp_cast_type FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/comp_cast_type.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb.company_name FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/company_name.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb.company_type FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/company_type.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb.complete_cast FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/complete_cast.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb.info_type FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/info_type.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb.keyword FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/keyword.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb.kind_type FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/kind_type.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb.link_type FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/link_type.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb.movie_companies FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/movie_companies.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb.movie_info_idx FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/movie_info_idx.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb.movie_keyword FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/movie_keyword.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb.movie_link FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/movie_link.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb.name FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/name.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb.role_type FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/role_type.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb.title FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/title.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb.movie_info FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/movie_info.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY imdb.person_info FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/person_info.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;

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