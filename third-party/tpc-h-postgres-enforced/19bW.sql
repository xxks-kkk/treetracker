/*+ Leading( (lineitem part) )
  */
select
   count(*)
from
   lineitem,
   part
where
   p_partkey = l_partkey
   and p_brand = 'Brand#23'
   and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
   and l_quantity >= 10
   and l_quantity <= 10 + 10
   and p_size between 1 and 10
   and l_shipmode in ('AIR', 'AIR REG')
   and l_shipinstruct = 'DELIVER IN PERSON';
