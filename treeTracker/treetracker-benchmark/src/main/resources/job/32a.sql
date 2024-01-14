-- implement the select predicates as views on top of the tables for JOB 32a
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/32a.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q32a_keyword CASCADE;
DROP VIEW IF EXISTS imdb.q32a_title2 CASCADE;

CREATE VIEW imdb.q32a_keyword as
SELECT keyword_id
FROM imdb.keyword as k
WHERE k.keyword ='10,000-mile-club';

/* Use kind_id will make Q32a become cyclic due to title1, title2, and movie_link.
   In specific, title1(movie_id, kind_id), title2(linked_movie_id, kind_id),
   and movie_link(movie_id, linked_movie_id, link_type_id). However, per Remy's comments,
   kind_id doesn't participate in the SQL (not part of join condition) and thus can be ignored.
   Thus, we rename kind_id to kind_id1 to avoid join consideration.
 */
CREATE VIEW imdb.q32a_title2 as
SELECT movie_id as linked_movie_id, kind_id as kind_id1
FROM imdb.title as t2;


END