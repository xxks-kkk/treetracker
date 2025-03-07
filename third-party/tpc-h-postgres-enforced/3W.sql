/*+ Leading( ((lineitem orders) customer) )
  */
select
   count(*)
from
   customer,
   orders,
   lineitem
where c_mktsegment = 'BUILDING'
      and c_custkey = o_custkey
      and l_orderkey = o_orderkey
      and o_orderdate < date '1995-03-15'
      and l_shipdate > date '1995-03-15';
