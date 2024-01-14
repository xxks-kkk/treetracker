-- implement the select predicates as views on top of the tables for SSB
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/ssb/1p3.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS ssb.q1p3_lineorder CASCADE;
DROP VIEW IF EXISTS ssb.q1p3_date CASCADE;

CREATE VIEW ssb.q1p3_lineorder as
SELECT LO_ORDERKEY, CUSTKEY, PARTKEY, SUPPKEY, DATEKEY
FROM ssb.lineorder
WHERE lo_discount between 5 and 7
  and lo_quantity between 36 and 40;

CREATE VIEW ssb.q1p3_date as
SELECT DATEKEY
FROM ssb.date
WHERE d_weeknuminyear = 6 and d_year = 1994;

END;
