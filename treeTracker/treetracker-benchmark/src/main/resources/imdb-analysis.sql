-- 31MB
SELECT pg_size_pretty(pg_total_relation_size('imdb.aka_name'));
-- 13MB
SELECT pg_size_pretty(pg_total_relation_size('imdb.aka_title'));
-- 1847MB (varchar)
SELECT pg_size_pretty(pg_total_relation_size('imdb.cast_info'));
-- 1531MB (int)
SELECT pg_size_pretty(pg_total_relation_size('imdb_int.cast_info'));
-- each row takes 16 bytes
SELECT pg_column_size(person_id) +
       pg_column_size(movie_id) +
       pg_column_size(person_role_id) +
       pg_column_size(role_id)
FROM imdb_int.cast_info
limit 1;
-- 109MB
SELECT pg_size_pretty(pg_total_relation_size('imdb.char_name'));
-- 16KB
SELECT pg_size_pretty(pg_total_relation_size('imdb.comp_cast_type'));
-- 8360KB
SELECT pg_size_pretty(pg_total_relation_size('imdb.company_name'));
-- 16KB
SELECT pg_size_pretty(pg_total_relation_size('imdb.company_type'));
-- 5888KB
SELECT pg_size_pretty(pg_total_relation_size('imdb.complete_cast'));
-- 16KB
SELECT pg_size_pretty(pg_total_relation_size('imdb.info_type'));
-- 4792KB
SELECT pg_size_pretty(pg_total_relation_size('imdb.keyword'));
-- 16KB
SELECT pg_size_pretty(pg_total_relation_size('imdb.kind_type'));
-- 16KB
SELECT pg_size_pretty(pg_total_relation_size('imdb.link_type'));
-- 112MB
SELECT pg_size_pretty(pg_total_relation_size('imdb.movie_companies'));
-- 58MB
SELECT pg_size_pretty(pg_total_relation_size('imdb.movie_info_idx'));
-- 191MB
SELECT pg_size_pretty(pg_total_relation_size('imdb.movie_keyword'));
-- 1440KB
SELECT pg_size_pretty(pg_total_relation_size('imdb.movie_link'));
-- 144MB
SELECT pg_size_pretty(pg_total_relation_size('imdb.name'));
-- 16KB
SELECT pg_size_pretty(pg_total_relation_size('imdb.role_type'));
-- 106MB
SELECT pg_size_pretty(pg_total_relation_size('imdb.title'));
-- 626MB
SELECT pg_size_pretty(pg_total_relation_size('imdb.movie_info'));
-- 125MB
SELECT pg_size_pretty(pg_total_relation_size('imdb.person_info'));
-- 1380035
select count(*) from imdb_int.movie_info_idx;
-- 14835720
select count(*) from imdb_int.movie_info;

---- Query 08c ----
--- cast_info \leftsemijoin aka_name
-- distinct no-good: 3555217
select count(distinct person_id)
from imdb_int.cast_info
where person_id not in (select person_id from imdb_int.aka_name);
-- duplicates (no-good): 16406381
select count(person_id) - count(distinct person_id)
from imdb_int.cast_info
where person_id not in (select person_id from imdb_int.aka_name);

--- aka_name \leftsemijoin cast_info
-- number of no-good: 129151
select count(person_id)
from imdb_int.aka_name
where person_id not in (select person_id from imdb_int.cast_info);

--- cast_info \leftsemijoin name
-- duplicates (no-good): 0
select count(person_id) - count(distinct person_id)
from imdb_int.cast_info
where person_id not in (select person_id from imdb_int.name);

--- cast_info \antijoin movie_companies
-- distinct no-good: 1291482
select count(distinct movie_id)
from imdb_int.cast_info
where movie_id not in (select movie_id from imdb_int.movie_companies);
-- duplicates (no-good): 14357989
select count(movie_id) - count(distinct movie_id)
from imdb_int.cast_info
where movie_id not in (select movie_id from imdb_int.movie_companies);

--- cast_info \leftsemijoin title
-- duplicates (no-good): 0
select count(movie_id) - count(distinct movie_id)
from imdb_int.cast_info
where movie_id not in (select movie_id from imdb_int.title);

--- cast_info \leftsemijoin role_type
-- duplicates (no-good): 0
select count(role_id) - count(distinct role_id)
from imdb_int.cast_info
where role_id not in (select role_id from imdb_int.role_type);

--- name \leftsemijoin aka_name
-- duplicates (no-good): 0
select count(person_id) - count(distinct person_id)
from imdb_int.name
where person_id not in (select person_id from imdb_int.aka_name);

--- movie_companies \antijoin company_name
-- no-good: 0
select count(company_id)
from imdb_int.movie_companies
where company_id not in (select company_id from imdb_int.company_name);

--- movie_companies \leftsemijoin title
-- duplicates (no-good): 0
select count(movie_id) - count(distinct movie_id)
from imdb_int.movie_companies
where movie_id not in (select movie_id from imdb_int.title);

---- Performance prediction on LIP vs. TTJ on 08c
--- total number of tuples of cast_info: 36244344
select count(person_id)
from imdb_int.cast_info;

--- number of good tuples: 9786726 (27% of total rows in cast_info)
select ((select count(person_id) from imdb_int.cast_info) -
        (select count(person_id) from imdb_int.cast_info where person_id not in (select person_id from imdb_int.aka_name)) -
        (select count(movie_id) from imdb_int.cast_info where movie_id not in (select movie_id from imdb_int.movie_companies)) +
        (select count(person_id)
         from imdb_int.cast_info
         where movie_id not in (select movie_id from imdb_int.movie_companies)
           and person_id not in (select person_id from imdb_int.aka_name)));

--- number of no-good tuples: 26457618 (73% of total rows in cast_info)
select ((select count(person_id) from imdb_int.cast_info where person_id not in (select person_id from imdb_int.aka_name)) +
        (select count(movie_id) from imdb_int.cast_info where movie_id not in (select movie_id from imdb_int.movie_companies)) -
        (select count(person_id)
         from imdb_int.cast_info
         where movie_id not in (select movie_id from imdb_int.movie_companies)
           and person_id not in (select person_id from imdb_int.aka_name)));
-- alternative query
select count(*)
from imdb_int.cast_info
where person_id not in (select person_id from imdb_int.aka_name)
   or movie_id not in (select movie_id from imdb_int.movie_companies);

--- cost of no-good discovery in vanilla TTJ (i.e., number of distinct no-good): 24337755 (67% of total rows in cast_info)
--- remarks: pay the cost of 67% of total rows and gains additional 6% (73% - 67%) of total rows filtered in cast_info.
--- TTJ is beneficial as long as the cost to pay > the gains from filtering, which in this case is not because 67% > 6%
--- (this gives a performance indicator, we can add an experiment to verify this).
--- 24337755 is also the size of ng. We cannot use Bloom filter to represent ng because false positive means the tuple is filtered out,
--- which leads to incorrect join result.
select count(*)
FROM (select 1
      from imdb_int.cast_info
      where person_id not in (select person_id from imdb_int.aka_name)
         or movie_id not in (select movie_id from imdb_int.movie_companies)
      group by person_id, movie_id) dt;

--- no-good that can be filtered out by vanilla TTJ: 2119863 (~6% of total rows in cast_info; 8% of total no-good tuples)
select ((select count(*)
         from imdb_int.cast_info
         where person_id not in (select person_id from imdb_int.aka_name)
            or movie_id not in (select movie_id from imdb_int.movie_companies)) -
        (select count(*)
         FROM (select 1
               from imdb_int.cast_info
               where person_id not in (select person_id from imdb_int.aka_name)
                  or movie_id not in (select movie_id from imdb_int.movie_companies)
               group by person_id, movie_id) dt));

--- no-good that can be filtered out by the high Perf TTJ: 21861827 (60% of total rows in cast_info; 83% of total no-good tuples)
--- In this case, we pay 13% of total rows to gain 60% of total rows filtered, which in favor of TTJHP.
select name, setting, unit, source from pg_settings where name = 'work_mem'; -- check work_mem
SET work_mem = '2040MB'; -- expensive query; we need to ensure Postgres has large work_mem
with allpersonid as (select person_id from imdb_int.aka_name),
     allmovieid as (select movie_id from imdb_int.movie_companies),
     tmp1 as (select person_id
              from imdb_int.cast_info
              where person_id not in (select person_id from allpersonid))
select (
-- count the number of no-good that can be filtered out due to the person_id
                       (select count(*)
                        from tmp1) -
                       (select count(*)
                        FROM (select 1
                              from imdb_int.cast_info
                              where person_id not in (select person_id from allpersonid)
                              group by person_id) dt) +
-- count the number of no-good that can be filtered out due to the movie_id
                       (select count(*)
                        from imdb_int.cast_info
                        where movie_id not in (select movie_id from allmovieid)) -
                       (select count(*)
                        from imdb_int.cast_info
                        where movie_id not in (select movie_id from allmovieid)
                          and person_id in (select person_id from tmp1)) -
                       (select count(distinct movie_id)
                        from imdb_int.cast_info
                        where movie_id not in (select movie_id from allmovieid)
                          and person_id not in (select person_id from tmp1)));


---- Query 08c4 ----
--- company_name \antijoin movie_companies
-- distinct no-good: 0
select count(distinct company_id)
from imdb_int.company_name
where company_id not in (select movie_companies.company_id from imdb_int.movie_companies);

--- movie_companies \antijoin cast_info
-- number of no-good: 89968
select count(movie_id)
from imdb_int.movie_companies
where movie_id not in (select cast_info.movie_id from imdb_int.cast_info);

--- cast_info \antijoin name
-- no-good: 0
select count(person_id)
from imdb_int.cast_info
where person_id not in (select person_id from imdb_int.name);

--- name \antijoin aka_name
-- distinct no-good: 3579269
select count(distinct person_id)
from imdb_int.name
where person_id not in (select person_id from imdb_int.aka_name);
-- duplicates (no-good): 3579269
select count(person_id)
from imdb_int.name
where person_id not in (select person_id from imdb_int.aka_name);

--- cast_info \antijoin title
-- duplicates (no-good): 0
select count(movie_id)
from imdb_int.cast_info
where movie_id not in (select movie_id from imdb_int.title);

--- cast_info \antijoin role_type
-- no-good: 0
select count(role_id)
from imdb_int.cast_info
where role_id not in (select role_id from imdb_int.role_type);

--- movie_companies \antijoin title
-- no-good: 0
select count(movie_id)
from imdb_int.movie_companies
where movie_id not in (select movie_id from imdb_int.title);

---- Query 08c3 ----
--- movie_companies \antijoin cast_info
-- no-good: 89968
select count(movie_id)
from imdb_int.movie_companies
where movie_id not in (select movie_id from imdb_int.cast_info);
-- distinct no-good: 47117
select count(distinct movie_id)
from imdb_int.movie_companies
where movie_id not in (select movie_id from imdb_int.cast_info);

---- Query q33a ----
--- movie_link \antijoin title
-- no-good: 0
select count(movie_id)
from imdb_int.movie_link
where movie_id not in (select movie_id from imdb_int.title);
--- movie_link \antijoin link_type
-- no-good: 0
select count(link_type_id)
from imdb_int.movie_link
where link_type_id not in (select link_type_id from imdb_int.link_type);
--- (title \join movie_link) \antijoin movie_info_idx
-- no-good: 9107
with title_join_movie_link as (select title.movie_id from imdb_int.title inner join imdb_int.movie_link ml on title.movie_id = ml.movie_id)
select count(movie_id)
from title_join_movie_link
where movie_id not in (select movie_id from imdb_int.movie_info_idx);
--- title \antijoin kind_type
-- no-good: 0
select count(kind_id)
from imdb_int.title
where kind_id not in (select kind_id from imdb_int.kind_type);
--- (movie_link \join title \join movie_info_idx) \antijoin movie_companies
-- no-good: 8409
with ml_title_mif as (select mii.movie_id from imdb_int.movie_link inner join imdb_int.title t on movie_link.movie_id = t.movie_id
    inner join imdb_int.movie_info_idx mii on movie_link.movie_id = mii.movie_id)
select count(movie_id)
from ml_title_mif
where movie_id not in (select movie_id from imdb_int.movie_companies);
--- movie_info_idx \antijoin info_type
-- no-good: 0
select count(info_type_id)
from imdb_int.movie_info_idx
where info_type_id not in (select info_type_id from imdb_int.info_type);

---- Query 31 ----
-- 0 --
SET work_mem = '2040MB';
select count(*)
from imdb_int.cast_info natural join
     imdb_int.name natural join
     imdb_int.movie_keyword natural join
     imdb_int.keyword natural join
     imdb_int.movie_info natural join
     imdb_int.info_type natural join
     imdb_int.title natural join
     imdb_int.movie_info_idx natural join
     imdb_int.movie_companies natural join
     imdb_int.company_name;

-- 0 --
select count(*)
from imdb_int.movie_companies natural join
     imdb_int.cast_info natural join
     imdb_int.name natural join
     imdb_int.movie_keyword natural join
     imdb_int.keyword natural join
     imdb_int.company_name natural join
     imdb_int.movie_info natural join
     imdb_int.movie_info_idx natural join
     imdb_int.info_type;

-- 0 --
select count(*)
from imdb_int.movie_companies natural join
     imdb_int.cast_info natural join
     imdb_int.name natural join
     imdb_int.movie_keyword natural join
     imdb_int.keyword natural join
     imdb_int.company_name natural join
     imdb_int.movie_info natural join
     imdb_int.movie_info_idx;

-- 0 --
select count(*)
from imdb_int.movie_companies natural join
     imdb_int.cast_info natural join
     imdb_int.movie_keyword natural join
     imdb_int.movie_info_idx natural join
     imdb_int.movie_info;

-- 0 --
select count(person_id)
from imdb_int.cast_info
where person_id not in (select name.person_id from imdb_int.name);
-- 0 --
select count(keyword_id)
from imdb_int.movie_keyword
where keyword_id not in (select keyword.keyword_id from imdb_int.keyword);

select count(info_type_id)
from imdb_int.movie_info_idx
where info_type_id not in (select info_type.info_type_id from imdb_int.info_type);

drop view imdb_int.mc_i;
create or replace view imdb_int.mc_i as select *
from imdb_int.movie_companies
where movie_id in (select movie_id from imdb_int.cast_info);

create or replace view imdb_int.mc_ii as select *
from imdb_int.mc_i
where movie_id in (select movie_id from imdb_int.movie_keyword);

select count(*) from imdb_int.mc_ii;

create or replace view imdb_int.mc_iv as select *
from imdb_int.mc_ii
where movie_id in (select movie_id from imdb_int.movie_info);

select count(*) from imdb_int.mc_iv;

select count(*)
from imdb_int.mc_ii
where movie_id in (select movie_id from imdb_int.movie_info);

create or replace view imdb_int.mc_v as select *
from imdb_int.mc_iv
where movie_id in (select movie_id from imdb_int.movie_info_idx);

select count(*) from imdb_int.mc_v;

select count(*)
from imdb_int.mc_v
where movie_id not in (select movie_id from imdb_int.title);

create or replace view imdb_int.ci_ii as select *
from imdb_int.cast_info
where movie_id in (select movie_id from imdb_int.mc_v);

create or replace view imdb_int.mk_ii as select *
from imdb_int.movie_keyword
where movie_id in (select movie_id from imdb_int.mc_v);

create or replace view imdb_int.cn_i as select count(*)
from imdb_int.company_name
where company_id in (select company_id from imdb_int.mc_v);

create or replace view imdb_int.mi_i as select *
from imdb_int.movie_info
where movie_id in (select movie_id from imdb_int.mc_v);

create or replace view imdb_int.mii_ii as select *
from imdb_int.movie_info_idx
where movie_id in (select movie_id from imdb_int.mc_v);

create or replace view imdb_int.t_i as select *
from imdb_int.title
where movie_id in (select movie_id from imdb_int.mc_v);

create or replace view imdb_int.n_i as select *
from imdb_int.name
where person_id in (select person_id from imdb_int.ci_ii);

select count(*)
from imdb_int.keyword
where keyword_id in (select keyword_id from imdb_int.mk_ii);

select count(*)
from imdb_int.info_type
where info_type_id in (select info_type_id from imdb_int.mii_ii)

select *
from imdb_int.ci_ii natural join imdb_int.mc_v natural join imdb_int.mii_ii natural join imdb_int.mi_i natural join imdb_int.mk_ii;

-- Q1a
-- 1
select count(*) from imdb.q1a_company_type;
-- 1
select count(*) from imdb.q1a_info_type;
-- 28889
select count(*) from imdb.q1a_movie_companies;
-- 2528312
select count(*) from imdb.title;
-- 1380035
select count(*) from imdb.movie_info_idx;
-- 14835720
select count(*) from imdb.movie_info;
-- 28657
select count(*) from imdb.q1a_company_type natural join imdb.q1a_movie_companies;
-- 28889
select count(*) from imdb.q1a_movie_companies natural join imdb.title;
-- 62658
select count(*) from imdb.q1a_movie_companies natural join imdb.movie_info_idx;
-- 1380035
select count(*) from imdb.title natural join imdb.movie_info_idx;
-- 250
select count(*) from imdb.q1a_info_type natural join imdb.movie_info_idx;