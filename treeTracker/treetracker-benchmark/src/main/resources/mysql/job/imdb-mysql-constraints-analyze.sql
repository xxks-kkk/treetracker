START TRANSACTION;

ALTER TABLE imdb.aka_name
    ADD CONSTRAINT fk_aka_name_name FOREIGN KEY (person_id) REFERENCES imdb.name (id);
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
    ADD CONSTRAINT fk_cast_info_title FOREIGN KEY (movie_id) REFERENCES imdb.title(id),
    -- Paper figure has bug: should reference imdb.name instead of aka_name
    ADD CONSTRAINT fk_cast_info_aka_name FOREIGN KEY (person_id) REFERENCES imdb.name(id),
    -- Due to convert Postgres semantics to TTJ semantics (replacing NULL with 0), this constraint no longer holds
--     ADD CONSTRAINT fk_cast_info_char_name FOREIGN KEY (person_role_id) REFERENCES imdb.char_name(person_role_id),
    ADD CONSTRAINT fk_cast_info_role_type FOREIGN KEY (role_id) REFERENCES imdb.role_type(id);
ALTER TABLE imdb.complete_cast
    ADD CONSTRAINT fk_complete_cast_comp_cast_type FOREIGN KEY (subject_id) REFERENCES imdb.comp_cast_type(id),
    ADD CONSTRAINT fk_complete_cast_comp_cast_type2 FOREIGN KEY (status_id) REFERENCES imdb.comp_cast_type(id),
    ADD CONSTRAINT fk_complete_cast_title FOREIGN KEY (movie_id) REFERENCES imdb.title(id);
ALTER TABLE imdb.movie_companies
    ADD CONSTRAINT fk_movie_companies_company_name FOREIGN KEY (company_id) REFERENCES imdb.company_name(id),
    ADD CONSTRAINT fk_movie_companies_title FOREIGN KEY (movie_id) REFERENCES imdb.title (id),
    ADD CONSTRAINT fk_movie_companies_company_type FOREIGN KEY (company_type_id) REFERENCES imdb.company_type(id);
ALTER TABLE imdb.movie_info_idx
    ADD CONSTRAINT fk_movie_info_idx_title FOREIGN KEY (movie_id) REFERENCES imdb.title(id),
    ADD CONSTRAINT fk_movie_info_idx_info_type FOREIGN KEY (info_type_id) REFERENCES imdb.info_type(id);
ALTER TABLE imdb.movie_keyword
    ADD CONSTRAINT fk_movie_keyword_title FOREIGN KEY (movie_id) REFERENCES imdb.title(id),
    ADD CONSTRAINT fk_movie_keyword_keyword FOREIGN KEY (keyword_id) REFERENCES imdb.keyword(id);
ALTER TABLE imdb.movie_link
    ADD CONSTRAINT fk_movie_link_title FOREIGN KEY (linked_movie_id) REFERENCES imdb.title(id),
    ADD CONSTRAINT fk_movie_link_title2 FOREIGN KEY (movie_id) REFERENCES imdb.title(id),
    ADD CONSTRAINT fk_movie_link_link_type FOREIGN KEY (link_type_id) REFERENCES imdb.link_type(id);
ALTER TABLE imdb.title
    ADD CONSTRAINT fk_title_kind_type FOREIGN KEY (kind_id) REFERENCES imdb.kind_type (id);
ALTER TABLE imdb.movie_info
    ADD CONSTRAINT fk_movie_info_title FOREIGN KEY (movie_id) REFERENCES imdb.title(id),
    ADD CONSTRAINT fk_movie_info_info_type FOREIGN KEY (info_type_id) REFERENCES imdb.info_type(id);
ALTER TABLE imdb.person_info
    ADD CONSTRAINT fk_person_info_info_type FOREIGN KEY (info_type_id) REFERENCES imdb.info_type(id),
    ADD CONSTRAINT fk_person_info_name FOREIGN KEY (person_id) REFERENCES imdb.name(id);

COMMIT;

ANALYZE TABLE imdb.aka_name;
ANALYZE TABLE imdb.aka_title;
ANALYZE TABLE imdb.cast_info;
ANALYZE TABLE imdb.char_name;
ANALYZE TABLE imdb.comp_cast_type;
ANALYZE TABLE imdb.company_name;
ANALYZE TABLE imdb.company_type;
ANALYZE TABLE imdb.complete_cast;
ANALYZE TABLE imdb.info_type;
ANALYZE TABLE imdb.keyword;
ANALYZE TABLE imdb.kind_type;
ANALYZE TABLE imdb.link_type;
ANALYZE TABLE imdb.movie_companies;
ANALYZE TABLE imdb.movie_info_idx;
ANALYZE TABLE imdb.movie_keyword;
ANALYZE TABLE imdb.movie_link;
ANALYZE TABLE imdb.name;
ANALYZE TABLE imdb.role_type;
ANALYZE TABLE imdb.title;
ANALYZE TABLE imdb.movie_info;
ANALYZE TABLE imdb.person_info;