create temp view if not exists lineitemrevenue (supplier_no, total_revenue) as
with a1 as (select l_suppkey,
                   sum(l_extendedprice * (1 - l_discount)) as total_revenue
            from lineitem
            where l_shipdate >= date('1996-01-01')
                  and l_shipdate < date('1996-01-01', '+3 month')
            group by l_suppkey)
SELECT l_suppkey, total_revenue
FROM a1
WHERE total_revenue = (SELECT MAX(total_revenue) FROM a1);

select
    count(*)
from
    supplier,
    lineitemrevenue
where
    s_suppkey = supplier_no;
