select
    count(*)
from
    supplier,
    lineitem,
    orders,
    customer,
    nation n1,
    nation n2
where
    s_suppkey = l_suppkey
    and o_orderkey = l_orderkey
    and c_custkey = o_custkey
    and s_nationkey = n1.n_nationkey
    and c_nationkey = n2.n_nationkey
    and n1.n_name = 'FRANCE'
    and n2.n_name = 'GERMANY'
    and l_shipdate between date '1995-01-01' and date '1996-12-31';
