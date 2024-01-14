-- implement the select predicates as views on top of the tables for SSB
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/ssb/1p1.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS ssb.q1p1_lineorder CASCADE;
DROP VIEW IF EXISTS ssb.q1p1_date CASCADE;

CREATE VIEW ssb.q1p1_lineorder as
SELECT LO_ORDERKEY, CUSTKEY, PARTKEY, SUPPKEY, DATEKEY
FROM ssb.lineorder
WHERE lo_discount between 1 and 3
  and lo_quantity < 25;

CREATE VIEW ssb.q1p1_date as
SELECT DATEKEY
FROM ssb.date
WHERE d_year = 1993;

END;
