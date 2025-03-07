/**
  Some SQLs to check the integrity of the ingested data
 */

 /**
   Check if there is any null values in the relations. There should not be after
   the execution of imdb-abbreviated-int-update-null.sql
   Ref: https://stackoverflow.com/a/73403180/1460102
  */
select count(*) from aka_name where person_id IS NULL;
select count(*) from aka_title where movie_id IS NULL;
select count(*) from cast_info where person_id + movie_id + person_role_id + role_id IS NULL;
select count(*) from char_name where person_role_id IS NULL;
select count(*) from comp_cast_type where subject_id IS NULL;
select count(*) from company_name where company_id IS NULL;
select count(*) from company_type where company_type_id IS NULL;
select count(*) from complete_cast where movie_id + subject_id + status_id IS NULL;
select count(*) from info_type where info_type_id IS NULL;
select count(*) from keyword where keyword_id IS NULL;
select count(*) from kind_type where kind_id IS NULL;
select count(*) from link_type where link_type_id IS NULL;
select count(*) from movie_companies where movie_id + company_id + company_type_id IS NULL;
select count(*) from movie_info where movie_id + info_type_id IS NULL;
select count(*) from movie_info_idx where movie_id + info_type_id IS NULL;
select count(*) from movie_keyword where movie_id + keyword_id IS NULL;
select count(*) from movie_link where movie_id + linked_movie_id + link_type_id IS NULL;
select count(*) from name where person_id IS NULL;
select count(*) from person_info where person_id + info_type_id IS NULL;
select count(*) from role_type where role_id IS NULL;
select count(*) from title where movie_id + kind_id IS NULL;
