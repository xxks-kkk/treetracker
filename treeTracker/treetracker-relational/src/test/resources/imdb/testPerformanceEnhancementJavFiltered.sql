-- This script is to use testPerformanceEnhancementJavFiltered data to verify the correctness of
-- query analysis in imdb-analysis.sql
-- NOTE: all the stats are associated with cast_info table scan operator

-- number of no-good tuples: 11
select count(*)
from multiwaycomplex."testPerformanceEnhancementJavFiltered_cast_info"
where person_id not in (select person_id from multiwaycomplex."testPerformanceEnhancementJavFiltered_aka_name")
   or movie_id not in (select movie_id from multiwaycomplex."testPerformanceEnhancementJavFiltered_movie_companies");

-- -- number of distinct no-good tuples: 5
select count(*)
FROM (select 1
      from multiwaycomplex."testPerformanceEnhancementJavFiltered_cast_info"
      where person_id not in (select person_id from multiwaycomplex."testPerformanceEnhancementJavFiltered_aka_name")
         or movie_id not in (select movie_id from multiwaycomplex."testPerformanceEnhancementJavFiltered_movie_companies")
      group by person_id, movie_id) dt;

-- no-good that can be filtered out by vanilla TTJ: 6
select ((select count(*)
         from multiwaycomplex."testPerformanceEnhancementJavFiltered_cast_info"
         where person_id not in (select person_id from multiwaycomplex."testPerformanceEnhancementJavFiltered_aka_name")
            or movie_id not in (select movie_id from multiwaycomplex."testPerformanceEnhancementJavFiltered_movie_companies")) -
        (select count(*)
         FROM (select 1
               from multiwaycomplex."testPerformanceEnhancementJavFiltered_cast_info"
               where person_id not in (select person_id from multiwaycomplex."testPerformanceEnhancementJavFiltered_aka_name")
                  or movie_id not in (select movie_id from multiwaycomplex."testPerformanceEnhancementJavFiltered_movie_companies")
               group by person_id, movie_id) dt));

-- no-good that can be filtered out by the high Perf TTJ: 8
-- NOTE: optimally, this number should be 9, which depends on the distribution of tuples. In specific, if tuples are
-- scanned in (1,1),(3,1),(3,1),(1,2),(1,2),(1,2),(3,2),(3,2),(3,2),(3,2),(3,4),(5,2), the number is 9. However, if
-- tuples are scanned in (1,1),(3,1),(3,1),(5,2),(1,2),(1,2),(1,2),(3,2),(3,2),(3,2),(3,2),(5,4), the number is 8 because
-- (5,2) is discovered due to join failure with aka_name on person_id; movie_id = 2 cannot be used and another tuple
-- with movie_id = 2 need to be used to discover movie_id = 2 as no-good. For 9, (5,2) is used to discover movie_id = 2 is no-good
-- and thus, we can filter one more tuple.
select (
-- count the number of no-good that can be filtered out due to the person_id
(select count(*)
from multiwaycomplex."testPerformanceEnhancementJavFiltered_cast_info"
where person_id not in (select person_id from multiwaycomplex."testPerformanceEnhancementJavFiltered_aka_name")) -
(select count(*)
 FROM (select 1
       from multiwaycomplex."testPerformanceEnhancementJavFiltered_cast_info"
       where person_id not in (select person_id from multiwaycomplex."testPerformanceEnhancementJavFiltered_aka_name")
       group by person_id) dt) +
-- count the number of no-good that can be filtered out due to the movie_id
(select count(*)
from multiwaycomplex."testPerformanceEnhancementJavFiltered_cast_info"
where movie_id not in (select movie_id from multiwaycomplex."testPerformanceEnhancementJavFiltered_movie_companies")) -
(select count(*)
from multiwaycomplex."testPerformanceEnhancementJavFiltered_cast_info"
where movie_id not in (select movie_id from multiwaycomplex."testPerformanceEnhancementJavFiltered_movie_companies")
  and person_id in (select person_id
                    from multiwaycomplex."testPerformanceEnhancementJavFiltered_cast_info"
                    where person_id not in (select person_id from multiwaycomplex."testPerformanceEnhancementJavFiltered_aka_name"))) -
(select count(distinct movie_id)
from multiwaycomplex."testPerformanceEnhancementJavFiltered_cast_info"
where movie_id not in (select movie_id from multiwaycomplex."testPerformanceEnhancementJavFiltered_movie_companies")
  and person_id not in (select person_id
                        from multiwaycomplex."testPerformanceEnhancementJavFiltered_cast_info"
                        where person_id not in (select person_id from multiwaycomplex."testPerformanceEnhancementJavFiltered_aka_name"))));