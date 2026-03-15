START TRANSACTION;

DROP TABLE IF EXISTS name;
DROP TABLE IF EXISTS aka_name;
DROP TABLE IF EXISTS aka_title;
DROP TABLE IF EXISTS cast_info;
DROP TABLE IF EXISTS char_name;
DROP TABLE IF EXISTS comp_cast_type;
DROP TABLE IF EXISTS company_name;
DROP TABLE IF EXISTS company_type;
DROP TABLE IF EXISTS complete_cast;
DROP TABLE IF EXISTS info_type;
DROP TABLE IF EXISTS keyword;
DROP TABLE IF EXISTS kind_type;
DROP TABLE IF EXISTS link_type;
DROP TABLE IF EXISTS movie_companies;
DROP TABLE IF EXISTS movie_info_idx;
DROP TABLE IF EXISTS movie_keyword;
DROP TABLE IF EXISTS movie_link;
DROP TABLE IF EXISTS role_type;
DROP TABLE IF EXISTS title;
DROP TABLE IF EXISTS movie_info;
DROP TABLE IF EXISTS person_info CASCADE;

CREATE TABLE name (
                                    id integer primary key ,
                                    name text NOT NULL,
                                    imdb_index character varying(12),
                                    imdb_id integer default NULL,
                                    gender character varying(1),
                                    name_pcode_cf character varying(5),
                                    name_pcode_nf character varying(5),
                                    surname_pcode character varying(5),
                                    md5sum character varying(32)
);


CREATE TABLE aka_name (
                                        id integer NOT NULL,
                                        person_id integer NOT NULL,
                                        name text NOT NULL,
                                        imdb_index character varying(12),
                                        name_pcode_cf character varying(5),
                                        name_pcode_nf character varying(5),
                                        surname_pcode character varying(5),
                                        md5sum character varying(32)
--                                         FOREIGN KEY (person_id) REFERENCES name (id)
);

CREATE TABLE aka_title (
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

CREATE TABLE kind_type (
                                         id integer primary key ,
                                         kind character varying(15) NOT NULL
);

CREATE TABLE title (
                                     id integer primary key,
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
--                                      FOREIGN KEY (kind_id) REFERENCES kind_type (id)
);

CREATE TABLE role_type (
                                         id integer primary key ,
                                         role character varying(32) NOT NULL
);

CREATE TABLE cast_info (
                                         id integer NOT NULL,
                                         person_id integer NOT NULL,
                                         movie_id integer NOT NULL,
                                         person_role_id integer,
                                         note text,
                                         nr_order integer,
                                         role_id integer NOT NULL
--                                          FOREIGN KEY (movie_id) REFERENCES title(id),
--                                          FOREIGN KEY (person_id) REFERENCES name(id),
--                                          FOREIGN KEY (role_id) REFERENCES role_type(id)
);

CREATE TABLE char_name (
                                         id integer primary key ,
                                         name text NOT NULL,
                                         imdb_index character varying(12),
                                         imdb_id integer,
                                         name_pcode_nf character varying(5),
                                         surname_pcode character varying(5),
                                         md5sum character varying(32)
);

CREATE TABLE comp_cast_type (
                                              id integer primary key,
                                              kind character varying(32) NOT NULL
);

CREATE TABLE company_name (
                                            id integer primary key,
                                            name text NOT NULL,
                                            country_code character varying(255),
                                            imdb_id integer,
                                            name_pcode_nf character varying(5),
                                            name_pcode_sf character varying(5),
                                            md5sum character varying(32)
);

CREATE TABLE company_type (
                                            id integer primary key,
                                            kind character varying(32) NOT NULL
);

CREATE TABLE complete_cast (
                                             id integer NOT NULL,
                                             movie_id integer NOT NULL,
                                             subject_id integer NOT NULL,
                                             status_id integer NOT NULL
--                                              FOREIGN KEY (subject_id) REFERENCES comp_cast_type(id),
--                                              FOREIGN KEY (status_id) REFERENCES comp_cast_type(id),
--                                              FOREIGN KEY (movie_id) REFERENCES title(id)
);

CREATE TABLE info_type (
                                         id integer primary key,
                                         info character varying(32) NOT NULL
);

CREATE TABLE keyword (
                                       id integer primary key,
                                       keyword text NOT NULL,
                                       phonetic_code character varying(5)
);

CREATE TABLE link_type (
                                         id integer primary key,
                                         link character varying(32) NOT NULL
);

CREATE TABLE movie_companies (
                                               id integer NOT NULL,
                                               movie_id integer NOT NULL,
                                               company_id integer NOT NULL,
                                               company_type_id integer NOT NULL,
                                               note text
--                                                FOREIGN KEY (company_id) REFERENCES company_name(id),
--                                                FOREIGN KEY (movie_id) REFERENCES title (id),
--                                                FOREIGN KEY (company_type_id) REFERENCES company_type(id)
);

CREATE TABLE movie_info_idx (
                                              id integer NOT NULL,
                                              movie_id integer NOT NULL,
                                              info_type_id integer NOT NULL,
                                              info text NOT NULL,
                                              note text
--                                               FOREIGN KEY (movie_id) REFERENCES title(id),
--                                               FOREIGN KEY (info_type_id) REFERENCES info_type(id)
);

CREATE TABLE movie_keyword (
                                             id integer NOT NULL,
                                             movie_id integer NOT NULL,
                                             keyword_id integer NOT NULL
--                                              FOREIGN KEY (movie_id) REFERENCES title(id),
--                                              FOREIGN KEY (keyword_id) REFERENCES keyword(id)
);

CREATE TABLE movie_link (
                                          id integer NOT NULL,
                                          movie_id integer NOT NULL,
                                          linked_movie_id integer NOT NULL,
                                          link_type_id integer NOT NULL
--                                           FOREIGN KEY (linked_movie_id) REFERENCES title(id),
--                                           FOREIGN KEY (movie_id) REFERENCES title(id),
--                                           FOREIGN KEY (link_type_id) REFERENCES link_type(id)
);

CREATE TABLE movie_info (
                                          id integer NOT NULL,
                                          movie_id integer NOT NULL,
                                          info_type_id integer NOT NULL,
                                          info text NOT NULL,
                                          note text
--                                           FOREIGN KEY (movie_id) REFERENCES title(id),
--                                           FOREIGN KEY (info_type_id) REFERENCES info_type(id)
);



CREATE TABLE person_info (
                                           id integer NOT NULL,
                                           person_id integer NOT NULL,
                                           info_type_id integer NOT NULL,
                                           info text NOT NULL,
                                           note text
--                                            FOREIGN KEY (info_type_id) REFERENCES info_type(id),
--                                            FOREIGN KEY (person_id) REFERENCES name(id)
);

COMMIT;