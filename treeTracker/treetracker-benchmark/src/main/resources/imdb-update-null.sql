/**
  setup Postgres database schemas of IMDB dataset for Join Ordering Benchmark (JOB). In specific, replace NULL
  with default value 0, which effectively implement TTJ semantics using Postgres SQL. In TTJ semantics, NULL
  is treated as 0 for INTEGER data type. To maintain the original dataset distribution, we increment non-zero
  values by 1, i.e., shifting the original data distribution. In addition, JOB queries have all the selections on
  the non-join attributes and our increment scheme only happens on the join attributes. Therefore, the original
  data distribution is intact.

  instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/imdb-update-null.sql
 */

BEGIN TRANSACTION;

UPDATE imdb.aka_name SET
    person_id = CASE WHEN person_id is NULL THEN 0
                     ELSE person_id + 1 END;
UPDATE imdb.aka_title SET
    movie_id = CASE WHEN movie_id is NULL THEN 0
                    ELSE movie_id + 1 END;
UPDATE imdb.cast_info SET
    person_id = CASE WHEN person_id is NULL THEN 0
                     ELSE person_id + 1 END,
    movie_id = CASE WHEN movie_id is NULL THEN 0
                    ELSE movie_id + 1 END,
    person_role_id = CASE WHEN person_role_id is NULL THEN 0
                          ELSE person_role_id + 1 END,
    role_id = CASE WHEN role_id is NULL THEN 0
                   ELSE role_id + 1 END;
UPDATE imdb.char_name SET
    person_role_id = CASE WHEN person_role_id is NULL THEN 0
                          ELSE person_role_id + 1 END;
UPDATE imdb.comp_cast_type SET
    subject_id = CASE WHEN subject_id is NULL THEN 0
                      ELSE subject_id + 1 END;
UPDATE imdb.company_name SET
    company_id = CASE WHEN company_id is NULL THEN 0
                      ELSE company_id + 1 END;
UPDATE imdb.company_type SET
    company_type_id = CASE WHEN company_type_id is NULL THEN 0
                           ELSE company_type_id + 1 END;
UPDATE imdb.complete_cast SET
    movie_id = CASE WHEN movie_id is NULL THEN 0
                    ELSE movie_id + 1 END,
    subject_id = CASE WHEN subject_id is NULL THEN 0
                      ELSE subject_id + 1 END,
    status_id = CASE WHEN status_id is NULL THEN 0
                     ELSE status_id + 1 END;
UPDATE imdb.info_type SET
    info_type_id = CASE WHEN info_type_id is NULL THEN 0
                        ELSE info_type_id + 1 END;
UPDATE imdb.keyword SET
    keyword_id = CASE WHEN keyword_id is NULL THEN 0
                      ELSE keyword_id + 1 END;
UPDATE imdb.kind_type SET
    kind_id = CASE WHEN kind_id is NULL THEN 0
                   ELSE kind_id + 1 END;
UPDATE imdb.link_type SET
    link_type_id = CASE WHEN link_type_id is NULL THEN 0
                        ELSE link_type_id + 1 END;
UPDATE imdb.movie_companies SET
    movie_id = CASE WHEN movie_id is NULL THEN 0
                    ELSE movie_id + 1 END,
    company_id = CASE WHEN company_id is NULL THEN 0
                      ELSE company_id + 1 END,
    company_type_id = CASE WHEN company_type_id is NULL THEN 0
                           ELSE company_type_id + 1 END;
UPDATE imdb.movie_info_idx SET
    movie_id = CASE WHEN movie_id is NULL THEN 0
                    ELSE movie_id + 1 END,
    info_type_id = CASE WHEN info_type_id is NULL THEN 0
                        ELSE info_type_id + 1 END;
UPDATE imdb.movie_keyword SET
    movie_id = CASE WHEN movie_id is NULL THEN 0
                    ELSE movie_id + 1 END,
    keyword_id = CASE WHEN keyword_id is NULL THEN 0
                      ELSE keyword_id + 1 END;
UPDATE imdb.movie_link SET
    movie_id = CASE WHEN movie_id is NULL THEN 0
                    ELSE movie_id + 1 END,
    linked_movie_id = CASE WHEN linked_movie_id is NULL THEN 0
                           ELSE linked_movie_id + 1 END,
    link_type_id = CASE WHEN link_type_id is NULL THEN 0
                        ELSE link_type_id + 1 END;
UPDATE imdb.name SET
    person_id = CASE WHEN person_id is NULL THEN 0
                     ELSE person_id + 1 END;
UPDATE imdb.role_type SET
    role_id = CASE WHEN role_id is NULL THEN 0
                   ELSE role_id + 1 END;
UPDATE imdb.title SET
    movie_id = CASE WHEN movie_id is NULL THEN 0
                    ELSE movie_id + 1 END,
    kind_id = CASE WHEN kind_id is NULL THEN 0
                   ELSE kind_id + 1 END;
UPDATE imdb.movie_info SET
    movie_id = CASE WHEN movie_id is NULL THEN 0
                    ELSE movie_id + 1 END,
    info_type_id = CASE WHEN info_type_id is NULL THEN 0
                        ELSE info_type_id + 1 END;
UPDATE imdb.person_info SET
    person_id = CASE WHEN person_id is NULL THEN 0
                     ELSE person_id + 1 END,
    info_type_id = CASE WHEN info_type_id is NULL THEN 0
                        ELSE info_type_id + 1 END;

COMMIT;

