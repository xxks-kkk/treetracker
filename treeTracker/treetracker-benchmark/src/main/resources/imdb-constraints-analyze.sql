/*
 Perform further setup on Postgres database schemas of IMDB dataset for Join Ordering Benchmark (JOB)
 including:
 - ANALYZE each table
 - Adding constraints following Fig.2 of "Query Optimization Through the Looking Glass, and What We Found
   Running the Join Order Benchmark"
 This script to be run after imdb.sql
 instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/imdb-constraints-analyze.sql
*/
ANALYZE imdb.aka_name;
ANALYZE imdb.aka_title;
ANALYZE imdb.cast_info;
ANALYZE imdb.char_name;
ANALYZE imdb.comp_cast_type;
ANALYZE imdb.company_name;
ANALYZE imdb.company_type;
ANALYZE imdb.complete_cast;
ANALYZE imdb.info_type;
ANALYZE imdb.keyword;
ANALYZE imdb.kind_type;
ANALYZE imdb.link_type;
ANALYZE imdb.movie_companies;
ANALYZE imdb.movie_info_idx;
ANALYZE imdb.movie_keyword;
ANALYZE imdb.movie_link;
ANALYZE imdb.name;
ANALYZE imdb.role_type;
ANALYZE imdb.title;
ANALYZE imdb.movie_info;
ANALYZE imdb.person_info;

BEGIN TRANSACTION;


ALTER TABLE imdb.aka_name
    ADD PRIMARY KEY (id);
ALTER TABLE imdb.aka_title
    ADD PRIMARY KEY (id);
ALTER TABLE imdb.cast_info
    ADD PRIMARY KEY (id);
ALTER TABLE imdb.char_name
    ADD PRIMARY KEY (person_role_id);
ALTER TABLE imdb.comp_cast_type
    ADD PRIMARY KEY (subject_id);
ALTER TABLE imdb.company_name
    ADD PRIMARY KEY (company_id);
ALTER TABLE imdb.company_type
    ADD PRIMARY KEY (company_type_id);
ALTER TABLE imdb.complete_cast
    ADD PRIMARY KEY (id);
ALTER TABLE imdb.info_type
    ADD PRIMARY KEY (info_type_id);
ALTER TABLE imdb.keyword
    ADD PRIMARY KEY (keyword_id);
ALTER TABLE imdb.kind_type
    ADD PRIMARY KEY (kind_id);
ALTER TABLE imdb.link_type
    ADD PRIMARY KEY (link_type_id);
ALTER TABLE imdb.movie_companies
    ADD PRIMARY KEY (id);
ALTER TABLE imdb.movie_info
    ADD PRIMARY KEY (id);
ALTER TABLE imdb.movie_info_idx
    ADD PRIMARY KEY (id);
ALTER TABLE imdb.movie_keyword
    ADD PRIMARY KEY (id);
ALTER TABLE imdb.movie_link
    ADD PRIMARY KEY (id);
ALTER TABLE imdb.name
    ADD PRIMARY KEY (person_id);
ALTER TABLE imdb.person_info
    ADD PRIMARY KEY (id);
ALTER TABLE imdb.role_type
    ADD PRIMARY KEY (role_id);
ALTER TABLE imdb.title
    ADD PRIMARY KEY (movie_id);

ALTER TABLE imdb.aka_name
    ADD CONSTRAINT fk_aka_name_name FOREIGN KEY (person_id) REFERENCES imdb.name (person_id);
/*
 The constraint doesn't hold in the raw data.
 Exception:
 ERROR:  insert or update on table "aka_title" violates foreign key constraint "fk_aka_title_title"
 DETAIL:  Key (movie_id)=(0) is not present in table "title".
 Examine: select * from imdb.aka_title where movie_id = 0;
 */
-- ALTER TABLE imdb.aka_title
--     ADD CONSTRAINT fk_aka_title_title FOREIGN KEY (movie_id) REFERENCES imdb.title(movie_id);
ALTER TABLE imdb.cast_info
    ADD CONSTRAINT fk_cast_info_title FOREIGN KEY (movie_id) REFERENCES imdb.title(movie_id),
    -- Paper figure has bug: should reference imdb.name instead of aka_name
    ADD CONSTRAINT fk_cast_info_aka_name FOREIGN KEY (person_id) REFERENCES imdb.name(person_id),
    -- Due to convert Postgres semantics to TTJ semantics (replacing NULL with 0), this constraint no longer holds
--     ADD CONSTRAINT fk_cast_info_char_name FOREIGN KEY (person_role_id) REFERENCES imdb.char_name(person_role_id),
    ADD CONSTRAINT fk_cast_info_role_type FOREIGN KEY (role_id) REFERENCES imdb.role_type(role_id);
ALTER TABLE imdb.complete_cast
    ADD CONSTRAINT fk_complete_cast_comp_cast_type FOREIGN KEY (subject_id) REFERENCES imdb.comp_cast_type(subject_id),
    ADD CONSTRAINT fk_complete_cast_comp_cast_type2 FOREIGN KEY (status_id) REFERENCES imdb.comp_cast_type(subject_id),
    ADD CONSTRAINT fk_complete_cast_title FOREIGN KEY (movie_id) REFERENCES imdb.title(movie_id);
ALTER TABLE imdb.movie_companies
    ADD CONSTRAINT fk_movie_companies_company_name FOREIGN KEY (company_id) REFERENCES imdb.company_name(company_id),
    ADD CONSTRAINT fk_movie_companies_title FOREIGN KEY (movie_id) REFERENCES imdb.title (movie_id),
    ADD CONSTRAINT fk_movie_companies_company_type FOREIGN KEY (company_type_id) REFERENCES imdb.company_type(company_type_id);
ALTER TABLE imdb.movie_info_idx
    ADD CONSTRAINT fk_movie_info_idx_title FOREIGN KEY (movie_id) REFERENCES imdb.title(movie_id),
    ADD CONSTRAINT fk_movie_info_idx_info_type FOREIGN KEY (info_type_id) REFERENCES imdb.info_type(info_type_id);
ALTER TABLE imdb.movie_keyword
    ADD CONSTRAINT fk_movie_keyword_title FOREIGN KEY (movie_id) REFERENCES imdb.title(movie_id),
    ADD CONSTRAINT fk_movie_keyword_keyword FOREIGN KEY (keyword_id) REFERENCES imdb.keyword(keyword_id);
ALTER TABLE imdb.movie_link
    ADD CONSTRAINT fk_movie_link_title FOREIGN KEY (linked_movie_id) REFERENCES imdb.title(movie_id),
    ADD CONSTRAINT fk_movie_link_title2 FOREIGN KEY (movie_id) REFERENCES imdb.title(movie_id),
    ADD CONSTRAINT fk_movie_link_link_type FOREIGN KEY (link_type_id) REFERENCES imdb.link_type(link_type_id);
ALTER TABLE imdb.title
    ADD CONSTRAINT fk_title_kind_type FOREIGN KEY (kind_id) REFERENCES imdb.kind_type (kind_id);
ALTER TABLE imdb.movie_info
    ADD CONSTRAINT fk_movie_info_title FOREIGN KEY (movie_id) REFERENCES imdb.title(movie_id),
    ADD CONSTRAINT fk_movie_info_info_type FOREIGN KEY (info_type_id) REFERENCES imdb.info_type(info_type_id);
ALTER TABLE imdb.person_info
    ADD CONSTRAINT fk_person_info_info_type FOREIGN KEY (info_type_id) REFERENCES imdb.info_type(info_type_id),
    ADD CONSTRAINT fk_person_info_name FOREIGN KEY (person_id) REFERENCES imdb.name(person_id);

COMMIT;