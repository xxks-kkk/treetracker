/*
 Perform further setup on Postgres database schemas of IMDB dataset for Join Ordering Benchmark (JOB)
 including:
 - ANALYZE each table
 - Adding constraints
 This script to be run after imdb-abbreviated-int.sql
 instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/imdb-abbreviated-int-constraints-analyze.sql
*/
ANALYZE imdb_int.aka_name;
ANALYZE imdb_int.aka_title;
ANALYZE imdb_int.cast_info;
ANALYZE imdb_int.char_name;
ANALYZE imdb_int.comp_cast_type;
ANALYZE imdb_int.company_name;
ANALYZE imdb_int.company_type;
ANALYZE imdb_int.complete_cast;
ANALYZE imdb_int.info_type;
ANALYZE imdb_int.keyword;
ANALYZE imdb_int.kind_type;
ANALYZE imdb_int.link_type;
ANALYZE imdb_int.movie_companies;
ANALYZE imdb_int.movie_info_idx;
ANALYZE imdb_int.movie_keyword;
ANALYZE imdb_int.movie_link;
ANALYZE imdb_int.name;
ANALYZE imdb_int.role_type;
ANALYZE imdb_int.title;
ANALYZE imdb_int.movie_info;
ANALYZE imdb_int.person_info;

BEGIN TRANSACTION;

ALTER TABLE imdb_int.name
    ADD PRIMARY KEY (person_id);
ALTER TABLE imdb_int.title
    ADD PRIMARY KEY (movie_id);
ALTER TABLE imdb_int.char_name
    ADD PRIMARY KEY (person_role_id);
ALTER TABLE imdb_int.role_type
    ADD PRIMARY KEY (role_id);
ALTER TABLE imdb_int.comp_cast_type
    ADD PRIMARY KEY (subject_id);
ALTER TABLE imdb_int.company_name
    ADD PRIMARY KEY (company_id);
ALTER TABLE imdb_int.company_type
    ADD PRIMARY KEY (company_type_id);
ALTER TABLE imdb_int.info_type
    ADD PRIMARY KEY (info_type_id);
ALTER TABLE imdb_int.keyword
    ADD PRIMARY KEY (keyword_id);
ALTER TABLE imdb_int.link_type
    ADD PRIMARY KEY (link_type_id);
ALTER TABLE imdb_int.kind_type
    ADD PRIMARY KEY (kind_id);

ALTER TABLE imdb_int.aka_name
    ADD CONSTRAINT fk_int_aka_name_name FOREIGN KEY (person_id) REFERENCES imdb_int.name (person_id);
ALTER TABLE imdb_int.cast_info
    ADD CONSTRAINT fk_int_cast_info_title FOREIGN KEY (movie_id) REFERENCES imdb_int.title(movie_id),
    -- Paper figure has bug: should reference imdb.name instead of aka_name
    ADD CONSTRAINT fk_int_cast_info_aka_name FOREIGN KEY (person_id) REFERENCES imdb_int.name(person_id),
    ADD CONSTRAINT fk_int_cast_info_role_type FOREIGN KEY (role_id) REFERENCES imdb_int.role_type(role_id);
ALTER TABLE imdb_int.complete_cast
    ADD CONSTRAINT fk_int_complete_cast_comp_cast_type FOREIGN KEY (subject_id) REFERENCES imdb_int.comp_cast_type(subject_id),
    ADD CONSTRAINT fk_int_complete_cast_comp_cast_type2 FOREIGN KEY (status_id) REFERENCES imdb_int.comp_cast_type(subject_id),
    ADD CONSTRAINT fk_int_complete_cast_title FOREIGN KEY (movie_id) REFERENCES imdb_int.title(movie_id);
ALTER TABLE imdb_int.movie_companies
    ADD CONSTRAINT fk_int_movie_companies_company_name FOREIGN KEY (company_id) REFERENCES imdb_int.company_name(company_id),
    ADD CONSTRAINT fk_int_movie_companies_title FOREIGN KEY (movie_id) REFERENCES imdb_int.title (movie_id),
    ADD CONSTRAINT fk_int_movie_companies_company_type FOREIGN KEY (company_type_id) REFERENCES imdb_int.company_type(company_type_id);
ALTER TABLE imdb_int.movie_info_idx
    ADD CONSTRAINT fk_int_movie_info_idx_title FOREIGN KEY (movie_id) REFERENCES imdb_int.title(movie_id),
    ADD CONSTRAINT fk_int_movie_info_idx_info_type FOREIGN KEY (info_type_id) REFERENCES imdb_int.info_type(info_type_id);
ALTER TABLE imdb_int.movie_keyword
    ADD CONSTRAINT fk_int_movie_keyword_title FOREIGN KEY (movie_id) REFERENCES imdb_int.title(movie_id),
    ADD CONSTRAINT fk_int_movie_keyword_keyword FOREIGN KEY (keyword_id) REFERENCES imdb_int.keyword(keyword_id);
ALTER TABLE imdb_int.movie_link
    ADD CONSTRAINT fk_int_movie_link_title FOREIGN KEY (linked_movie_id) REFERENCES imdb_int.title(movie_id),
    ADD CONSTRAINT fk_int_movie_link_title2 FOREIGN KEY (movie_id) REFERENCES imdb_int.title(movie_id),
    ADD CONSTRAINT fk_int_movie_link_link_type FOREIGN KEY (link_type_id) REFERENCES imdb_int.link_type(link_type_id);
ALTER TABLE imdb_int.title
    ADD CONSTRAINT fk_int_title_kind_type FOREIGN KEY (kind_id) REFERENCES imdb_int.kind_type (kind_id);
ALTER TABLE imdb_int.movie_info
    ADD CONSTRAINT fk_int_movie_info_title FOREIGN KEY (movie_id) REFERENCES imdb_int.title(movie_id),
    ADD CONSTRAINT fk_int_movie_info_info_type FOREIGN KEY (info_type_id) REFERENCES imdb_int.info_type(info_type_id);
ALTER TABLE imdb_int.person_info
    ADD CONSTRAINT fk_int_person_info_info_type FOREIGN KEY (info_type_id) REFERENCES imdb_int.info_type(info_type_id),
    ADD CONSTRAINT fk_int_person_info_name FOREIGN KEY (person_id) REFERENCES imdb_int.name(person_id);

COMMIT;