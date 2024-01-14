-- implement the select predicates as views on top of the tables for SSB
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/ssb/1p2.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS ssb.q1p2_lineorder CASCADE;
DROP VIEW IF EXISTS ssb.q1p2_date CASCADE;

CREATE VIEW ssb.q1p2_lineorder as
SELECT LO_ORDERKEY, CUSTKEY, PARTKEY, SUPPKEY, DATEKEY
FROM ssb.lineorder
WHERE lo_discount between 4 and 6
  and lo_quantity between 26 and 35;

CREATE VIEW ssb.q1p2_date as
SELECT DATEKEY
FROM ssb.date
WHERE d_yearmonthnum = 199401;

END;
