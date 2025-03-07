-- implement the select predicates as views on top of the tables for TPC-H
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/tpch/15.sql

BEGIN TRANSACTION;

DROP VIEW IF EXISTS tpch.q15w_lineitem CASCADE;

-- We rewrite the view in a different but equivalent definition because for some reason in DuckDB, if we use the original definition,
-- the view will be empty.
create view tpch.q15w_lineitem (suppkey, total_revenue) as
WITH a1 AS (
    SELECT
        suppkey,
        SUM(l_extendedprice * (1 - l_discount)) AS total_revenue,
        ROW_NUMBER() OVER (ORDER BY SUM(l_extendedprice * (1 - l_discount)) DESC) AS rn
    FROM
        tpch.lineitem
    WHERE
        l_shipdate >= DATE '1996-01-01'
      AND l_shipdate < DATE '1996-01-01' + INTERVAL '3' MONTH
    GROUP BY
        suppkey
)
SELECT
    suppkey,
    total_revenue
FROM
    a1
WHERE
    rn = 1;

END;