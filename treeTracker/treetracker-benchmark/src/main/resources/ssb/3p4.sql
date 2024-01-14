-- implement the select predicates as views on top of the tables for SSB
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/ssb/3p4.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS ssb.q3p4_customer CASCADE;
DROP VIEW IF EXISTS ssb.q3p4_supplier CASCADE;
DROP VIEW IF EXISTS ssb.q3p4_date CASCADE;

CREATE VIEW ssb.q3p4_customer as
SELECT custkey
FROM ssb.customer
WHERE c_city='UNITED KI1' or c_city='UNITED KI5';

CREATE VIEW ssb.q3p4_supplier as
SELECT suppkey
FROM ssb.supplier
WHERE s_city='UNITED KI1' or s_city='UNITED KI5';

CREATE VIEW ssb.q3p4_date as
SELECT DATEKEY
FROM ssb.date
WHERE d_yearmonth = 'Dec1997';

END;
