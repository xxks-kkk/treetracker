explain query plan
select
    sum(l_extendedprice * (1 - l_discount) ) as revenue
from
   lineitem,
   part
where
   p_partkey = l_partkey
   and p_brand = 'Brand#12'
   and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
   and l_quantity >= 1
   and l_quantity <= 1 + 10
   and p_size between 1 and 5
   and l_shipmode in ('AIR', 'AIR REG')
   and l_shipinstruct = 'DELIVER IN PERSON';
