-- implement the select predicates as views on top of the tables for SSB
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/ssb/2p3.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS ssb.q2p3_part CASCADE;
DROP VIEW IF EXISTS ssb.q2p3_supplier CASCADE;

CREATE VIEW ssb.q2p3_part as
SELECT PARTKEY
FROM ssb.part
WHERE p_brand1 = 'MFGR#2339';

CREATE VIEW ssb.q2p3_supplier as
SELECT suppkey
FROM ssb.supplier
WHERE s_region = 'EUROPE';

END;
