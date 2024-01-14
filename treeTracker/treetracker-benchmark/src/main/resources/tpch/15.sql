-- implement the select predicates as views on top of the tables for TPC-H
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/tpch/15.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS tpch.q15w_lineitem CASCADE;

create view tpch.q15w_lineitem (suppkey, total_revenue) as
with a1 as (select suppkey,
                   sum(l_extendedprice * (1 - l_discount)) as total_revenue
            from tpch.lineitem
            where l_shipdate >= date '1996-01-01'
              and l_shipdate < date '1996-01-01' + interval '3' month
            group by suppkey)
SELECT suppkey, total_revenue
FROM a1
WHERE total_revenue = (SELECT MAX(total_revenue) FROM a1);

END;