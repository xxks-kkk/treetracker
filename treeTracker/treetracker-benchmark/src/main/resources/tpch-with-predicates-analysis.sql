/**
  Check correctness of TPC-H query implementation. The count result is based on S1.
 */
-- Q3
--- 30519
select count(*)
from
    tpch.customer,
    tpch.orders,
    tpch.lineitem
where c_mktsegment = 'BUILDING'
  and customer.custkey = orders.custkey
  and lineitem.orderkey = orders.orderkey
  and o_orderdate < date '1995-03-15'
  and l_shipdate > date '1995-03-15';

select count(*)
from tpch.q3w_customer natural join
     tpch.q3w_orders natural join
     tpch.q3w_lineitem;

-- Q7a
--- 3014
select count(*)
from
    tpch.supplier,
    tpch.lineitem,
    tpch.orders,
    tpch.customer,
    tpch.nation n1,
    tpch.nation n2
where supplier.suppkey = lineitem.suppkey
  and orders.orderkey = lineitem.orderkey
  and customer.custkey = orders.custkey
  and supplier.nationkey = n1.nationkey
  and customer.nationkey = n2.nationkey
  and n1.n_name = 'FRANCE'
  and n2.n_name = 'GERMANY'
  and l_shipdate between date '1995-01-01' and date '1996-12-31';

select count(*)
from tpch.q7aw_nation1 natural join
     tpch.q7aw_nation2 natural join
     tpch.q7aw_customer natural join
     tpch.q7aw_lineitem natural join
    tpch_int.supplier natural join
    tpch_int.orders;

-- Q7b
--- 2910
select count(*)
from
    tpch.supplier,
    tpch.lineitem,
    tpch.orders,
    tpch.customer,
    tpch.nation n1,
    tpch.nation n2
where supplier.suppkey = lineitem.suppkey
  and orders.orderkey = lineitem.orderkey
  and customer.custkey = orders.custkey
  and supplier.nationkey = n1.nationkey
  and customer.nationkey = n2.nationkey
  and n1.n_name = 'GERMANY'
  and n2.n_name = 'FRANCE'
  and l_shipdate between date '1995-01-01' and date '1996-12-31';

select count(*)
from tpch.q7bw_nation1 natural join
     tpch.q7bw_nation2 natural join
     tpch.q7bw_customer natural join
     tpch.q7bw_lineitem natural join
     tpch_int.supplier natural join
     tpch_int.orders;

-- Q8
--- 2603
select count(*)
from
    tpch.part,
    tpch.supplier,
    tpch.lineitem,
    tpch.orders,
    tpch.customer,
    tpch.nation n1,
    tpch.nation n2,
    tpch.region
where part.partkey = lineitem.partkey
  and supplier.suppkey = lineitem.suppkey
  and lineitem.orderkey = orders.orderkey
  and orders.custkey = customer.custkey
  and customer.nationkey = n1.nationkey
  and n1.regionkey = region.regionkey
  and r_name = 'AMERICA'
  and supplier.nationkey = n2.nationkey
  and o_orderdate between date '1995-01-01' and date '1996-12-31'
  and p_type = 'ECONOMY ANODIZED STEEL';

select count(*)
from tpch.q8w_region natural join
    tpch.q8w_nation natural join
    tpch.q8w_nation2 natural join
    tpch.q8w_supplier natural join
    tpch.q8w_orders natural join
    tpch.q8w_part natural join
    tpch_int.lineitem natural join
    tpch_int.customer;

-- Q9
--- 319404
select count(*)
from
    tpch.part,
    tpch.supplier,
    tpch.lineitem,
    tpch.partsupp,
    tpch.orders,
    tpch.nation
where supplier.suppkey = lineitem.suppkey
  and partsupp.suppkey = lineitem.suppkey
  and partsupp.partkey = lineitem.partkey
  and part.partkey = lineitem.partkey
  and orders.orderkey = lineitem.orderkey
  and supplier.nationkey = nation.nationkey
  and p_name like '%green%';

select count(*)
from tpch.q9w_part natural join
     tpch_int.supplier natural join
     tpch_int.lineitem natural join
    tpch_int.partsupp natural join
    tpch_int.orders natural join
    tpch_int.nation;

-- Q10
--- 114705
select count(*)
from
    tpch.customer,
    tpch.orders,
    tpch.lineitem,
    tpch.nation
where customer.custkey = orders.custkey
  and lineitem.orderkey = orders.orderkey
  and o_orderdate >= date '1993-10-01'
  and o_orderdate < date '1993-10-01' + interval '3' month
  and l_returnflag = 'R'
  and customer.nationkey = nation.nationkey;

SELECT count(*)
FROM tpch.q10w_orders natural join
     tpch.q10w_lineitem natural join
     tpch_int.customer natural join
    tpch_int.nation;

-- Q11
--- 31680
select count(*)
from
    tpch.partsupp,
    tpch.supplier,
    tpch.nation
where partsupp.suppkey = supplier.suppkey
  and supplier.nationkey = nation.nationkey
  and n_name = 'GERMANY';

select count(*)
from tpch.q11w_nation natural join
    tpch_int.partsupp natural join
    tpch_int.supplier;

-- Q12
--- 30988
select count(*)
from
    tpch.orders,
    tpch.lineitem
where orders.orderkey = lineitem.orderkey
  and l_shipmode in ('MAIL', 'SHIP')
  and l_commitdate < l_receiptdate
  and l_shipdate < l_commitdate
  and l_receiptdate >= date '1994-01-01'
  and l_receiptdate < date '1994-01-01' + interval '1' year;

select count(*)
from tpch.q12w_lineitem natural join tpch_int.orders;

-- Q14
--- 75983
select count(*)
from tpch.lineitem,
     tpch.part
where lineitem.partkey = part.partkey
  and l_shipdate >= date '1995-09-01'
  and l_shipdate < date '1995-09-01' + interval '1' month;

select count(*)
from tpch.q14w_lineitem natural join
     tpch_int.part;

-- Q15
--- 1
create view tpch.revenue1 (supplier_no, total_revenue) as
select
    suppkey,
    sum(l_extendedprice * (1 - l_discount))
from
    tpch.lineitem
where l_shipdate >= date '1996-01-01'
  and l_shipdate < date '1996-01-01' + interval '3' month
group by
    suppkey;
select count(*)
from
    tpch.supplier,
    tpch.revenue1
where
    suppkey = supplier_no
and total_revenue = (
    select
        max(total_revenue)
    from
        tpch.revenue1
    );
drop view revenue1;

SELECT count(*)
FROM tpch.q15w_lineitem natural join
     tpch_int.supplier;

-- Q16
--- 118274
select count(*)
from
    tpch.partsupp,
    tpch.part
where part.partkey = partsupp.partkey
  and p_brand <> 'Brand#45'
  and p_type not like 'MEDIUM POLISHED%'
  and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
  and partsupp.suppkey not in (
    select suppkey
    from tpch.supplier
    where
        s_comment like '%Customer%Complaints%');

select count(*)
from tpch.q16w_partsupp natural join
     tpch.q16w_part;

-- Q18
--- 399
select count(*)
from
    tpch.customer,
    tpch.orders,
    tpch.lineitem
where
        orders.orderkey in (
        select
            lineitem.orderkey
        from
            lineitem
        group by
            lineitem.orderkey having
                sum(l_quantity) > 300)
  and customer.custkey = orders.custkey
  and orders.orderkey = lineitem.orderkey;

select count(*)
from tpch.q18w_orders natural join
    tpch_int.customer natural join
    tpch_int.lineitem;

-- Q19a
--- 25
select count(*)
from tpch.lineitem,
     tpch.part
where part.partkey = lineitem.partkey
and p_brand = 'Brand#12'
and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
and l_quantity >= 1
and l_quantity <= 1 + 10
and p_size between 1 and 5
and l_shipmode in ('AIR', 'AIR REG')
and l_shipinstruct = 'DELIVER IN PERSON';

select count(*)
from tpch.q19aw_lineitem natural join
    tpch.q19aw_part;

-- Q19b
--- 40
select count(*)
from tpch.lineitem,
     tpch.part
where part.partkey = lineitem.partkey
  and p_brand = 'Brand#23'
and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
and l_quantity >= 10
  and l_quantity <= 10 + 10
and p_size between 1 and 10
and l_shipmode in ('AIR', 'AIR REG')
and l_shipinstruct = 'DELIVER IN PERSON';

select count(*)
from tpch.q19bw_lineitem natural join tpch.q19bw_part;

-- Q19c
--- 56
select count(*)
from tpch.lineitem,
     tpch.part
where part.partkey = lineitem.partkey
  and p_brand = 'Brand#34'
and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
and l_quantity >= 20
and l_quantity <= 20 + 10
and p_size between 1 and 15
and l_shipmode in ('AIR', 'AIR REG')
and l_shipinstruct = 'DELIVER IN PERSON';

select count(*)
from tpch.q19cw_lineitem natural join
     tpch.q19cw_part;

-- Q20
--- 198
select count(*)
from
    tpch.supplier,
    tpch.nation
where
        supplier.suppkey in (
        select
            partsupp.suppkey
        from
            tpch.partsupp
        where
                partsupp.partkey in (
                select
                    part.partkey
                from
                    tpch.part
                where
                        p_name like 'forest%'
            )
          and ps_availqty > (
            select
                    0.5 * sum(l_quantity)
            from
                tpch.lineitem
            where
                    lineitem.partkey = partsupp.partkey
              and lineitem.suppkey = partsupp.suppkey
              and l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year
        )
    )
  and supplier.nationkey = nation.nationkey
  and n_name = 'CHINA';

select count(*)
from tpch.q20w_supplier natural join tpch.q20w_nation;