-- implement the select predicates as views on top of the tables for JOB 6f
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/job/6f.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS imdb.q6f_keyword CASCADE;
DROP VIEW IF EXISTS imdb.q6f_title CASCADE;

CREATE VIEW imdb.q6f_keyword as
SELECT keyword_id
FROM imdb.keyword as k
WHERE k.keyword IN ('superhero',
                    'sequel',
                    'second-part',
                    'marvel-comics',
                    'based-on-comic',
                    'tv-special',
                    'fight',
                    'violence');

CREATE VIEW imdb.q6f_title as
SELECT movie_id, kind_id
FROM imdb.title as t
WHERE t.production_year > 2000;


END