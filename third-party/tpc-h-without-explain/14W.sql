select
    count(*)
from
    lineitem,
    part
where
    l_partkey = p_partkey
    and l_shipdate >= date('1995-09-01')
    and l_shipdate < date('1995-09-01', '+1 month');
