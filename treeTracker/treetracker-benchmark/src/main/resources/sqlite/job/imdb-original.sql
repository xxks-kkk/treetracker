-- setup Postgres database schemas of IMDB dataset for Join Ordering Benchmark (JOB). We use original JOB schemas. This
-- is for SQLite data ingestion.
-- instruction: psql -p5432 -d imdb -f treeTracker/treetracker-benchmark/src/main/resources/sqlite/job/imdb-original.sql
BEGIN TRANSACTION;

DROP TABLE IF EXISTS aka_name, aka_title,cast_info,char_name,comp_cast_type,company_name,company_type,complete_cast,info_type,keyword,kind_type,link_type,movie_companies,movie_info,movie_info_idx,movie_keyword,movie_link,name,person_info,role_type,title;

CREATE UNLOGGED TABLE aka_name (
                          id integer NOT NULL PRIMARY KEY,
                          person_id integer NOT NULL,
                          name character varying,
                          imdb_index character varying(3),
                          name_pcode_cf character varying(11),
                          name_pcode_nf character varying(11),
                          surname_pcode character varying(11),
                          md5sum character varying(65)
);

CREATE UNLOGGED TABLE aka_title (
                           id integer NOT NULL PRIMARY KEY,
                           movie_id integer NOT NULL,
                           title character varying,
                           imdb_index character varying(4),
                           kind_id integer NOT NULL,
                           production_year integer,
                           phonetic_code character varying(5),
                           episode_of_id integer,
                           season_nr integer,
                           episode_nr integer,
                           note character varying(72),
                           md5sum character varying(32)
);

CREATE UNLOGGED TABLE cast_info (
                           id integer NOT NULL PRIMARY KEY,
                           person_id integer NOT NULL,
                           movie_id integer NOT NULL,
                           person_role_id integer,
                           note character varying,
                           nr_order integer,
                           role_id integer NOT NULL
);

CREATE UNLOGGED TABLE char_name (
                           id integer NOT NULL PRIMARY KEY,
                           name character varying NOT NULL,
                           imdb_index character varying(2),
                           imdb_id integer,
                           name_pcode_nf character varying(5),
                           surname_pcode character varying(5),
                           md5sum character varying(32)
);

CREATE UNLOGGED TABLE comp_cast_type (
                                id integer NOT NULL PRIMARY KEY,
                                kind character varying(32) NOT NULL
);

CREATE UNLOGGED TABLE company_name (
                              id integer NOT NULL PRIMARY KEY,
                              name character varying NOT NULL,
                              country_code character varying(6),
                              imdb_id integer,
                              name_pcode_nf character varying(5),
                              name_pcode_sf character varying(5),
                              md5sum character varying(32)
);

CREATE UNLOGGED TABLE company_type (
                              id integer NOT NULL PRIMARY KEY,
                              kind character varying(32)
);

CREATE UNLOGGED TABLE complete_cast (
                               id integer NOT NULL PRIMARY KEY,
                               movie_id integer,
                               subject_id integer NOT NULL,
                               status_id integer NOT NULL
);

CREATE UNLOGGED TABLE info_type (
                           id integer NOT NULL PRIMARY KEY,
                           info character varying(32) NOT NULL
);

CREATE UNLOGGED TABLE keyword (
                         id integer NOT NULL PRIMARY KEY,
                         keyword character varying NOT NULL,
                         phonetic_code character varying(5)
);

CREATE UNLOGGED TABLE kind_type (
                           id integer NOT NULL PRIMARY KEY,
                           kind character varying(15)
);

CREATE UNLOGGED TABLE link_type (
                           id integer NOT NULL PRIMARY KEY,
                           link character varying(32) NOT NULL
);

CREATE UNLOGGED TABLE movie_companies (
                                 id integer NOT NULL PRIMARY KEY,
                                 movie_id integer NOT NULL,
                                 company_id integer NOT NULL,
                                 company_type_id integer NOT NULL,
                                 note character varying
);

CREATE UNLOGGED TABLE movie_info_idx (
                                id integer NOT NULL PRIMARY KEY,
                                movie_id integer NOT NULL,
                                info_type_id integer NOT NULL,
                                info character varying NOT NULL,
                                note character varying(1)
);

CREATE UNLOGGED TABLE movie_keyword (
                               id integer NOT NULL PRIMARY KEY,
                               movie_id integer NOT NULL,
                               keyword_id integer NOT NULL
);

CREATE UNLOGGED TABLE movie_link (
                            id integer NOT NULL PRIMARY KEY,
                            movie_id integer NOT NULL,
                            linked_movie_id integer NOT NULL,
                            link_type_id integer NOT NULL
);

CREATE UNLOGGED TABLE name (
                      id integer NOT NULL PRIMARY KEY,
                      name character varying NOT NULL,
                      imdb_index character varying(9),
                      imdb_id integer,
                      gender character varying(1),
                      name_pcode_cf character varying(5),
                      name_pcode_nf character varying(5),
                      surname_pcode character varying(5),
                      md5sum character varying(32)
);

CREATE UNLOGGED TABLE role_type (
                           id integer NOT NULL PRIMARY KEY,
                           role character varying(32) NOT NULL
);

CREATE UNLOGGED TABLE title (
                       id integer NOT NULL PRIMARY KEY,
                       title character varying NOT NULL,
                       imdb_index character varying(5),
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

CREATE UNLOGGED TABLE movie_info (
                            id integer NOT NULL PRIMARY KEY,
                            movie_id integer NOT NULL,
                            info_type_id integer NOT NULL,
                            info character varying NOT NULL,
                            note character varying
);

CREATE UNLOGGED TABLE person_info (
                             id integer NOT NULL PRIMARY KEY,
                             person_id integer NOT NULL,
                             info_type_id integer NOT NULL,
                             info character varying NOT NULL,
                             note character varying
);

COPY aka_name FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/aka_name.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY aka_title FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/aka_title.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY cast_info FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/cast_info.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY char_name FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/char_name.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY comp_cast_type FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/comp_cast_type.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY company_name FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/company_name.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY company_type FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/company_type.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY complete_cast FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/complete_cast.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY info_type FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/info_type.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY keyword FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/keyword.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY kind_type FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/kind_type.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY link_type FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/link_type.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY movie_companies FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/movie_companies.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY movie_info_idx FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/movie_info_idx.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY movie_keyword FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/movie_keyword.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY movie_link FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/movie_link.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY name FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/name.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY role_type FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/role_type.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY title FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/title.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY movie_info FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/movie_info.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY person_info FROM '/home/zeyuanhu/projects/job/imdb2013/imdb/person_info.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;

COMMIT;