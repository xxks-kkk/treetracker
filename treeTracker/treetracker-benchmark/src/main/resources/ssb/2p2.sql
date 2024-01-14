-- implement the select predicates as views on top of the tables for SSB
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/ssb/2p2.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS ssb.q2p2_part CASCADE;
DROP VIEW IF EXISTS ssb.q2p2_supplier CASCADE;

CREATE VIEW ssb.q2p2_part as
SELECT PARTKEY
FROM ssb.part
WHERE p_brand1 between 'MFGR#2221' and 'MFGR#2228';

CREATE VIEW ssb.q2p2_supplier as
SELECT suppkey
FROM ssb.supplier
WHERE s_region = 'ASIA';

END;
