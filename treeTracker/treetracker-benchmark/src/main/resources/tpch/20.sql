-- implement the select predicates as views on top of the tables for TPC-H
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/tpch/20.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS tpch.q20w_supplier CASCADE;
DROP VIEW IF EXISTS tpch.q20w_nation CASCADE;

CREATE VIEW tpch.q20w_supplier AS
select suppkey, nationkey
from tpch.supplier
where suppkey in (select suppkey as ps_suppkey
                  from tpch.partsupp
                  where tpch.partsupp.partkey in (select partkey
                                                  from tpch.part
                                                  where p_name like 'forest%')
                    and ps_availqty > (select 0.5 * sum(l_quantity)
                                       from tpch.lineitem
                                       where lineitem.partkey = partsupp.partkey
                                         and lineitem.suppkey = partsupp.suppkey
                                         and l_shipdate >= date '1994-01-01'
                                         and l_shipdate < date '1994-01-01' + interval '1' year));

CREATE VIEW tpch.q20w_nation AS
select nationkey, regionkey
from tpch.nation
where n_name = 'CHINA';

END;