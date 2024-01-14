-- implement the select predicates as views on top of the tables for TPC-H
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/tpch/16.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS tpch.q16w_partsupp CASCADE;
DROP VIEW IF EXISTS tpch.q16w_part CASCADE;

create view tpch.q16w_partsupp as
SELECT partkey, suppkey
FROM tpch.partsupp
WHERE suppkey not in (select suppkey
                      from tpch.supplier
                      where s_comment like '%Customer%Complaints%');

CREATE VIEW tpch.q16w_part as
SELECT partkey
FROM tpch.part
WHERE p_brand <> 'Brand#45'
  and p_type not like 'MEDIUM POLISHED%'
  and p_size in (49, 14, 23, 45, 19, 3, 36, 9);

END;