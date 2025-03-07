/**
  MySQL orders on TPC-H queries that their SQLite orders have cross-product.
 */
-- 7aW
/**
  mysql> explain select
    ->     n1.n_name as supp_nation,
    ->     n2.n_name as cust_nation,
    ->     extract(year from l_shipdate) as l_year,
    ->     l_extendedprice * (1 - l_discount) as volume
    -> from
    ->     supplier,
    ->     lineitem,
    ->     orders,
    ->     customer,
    ->     nation n1,
    ->     nation n2
    -> where
    ->     s_suppkey = l_suppkey
    ->   and o_orderkey = l_orderkey
    ->   and c_custkey = o_custkey
    ->   and s_nationkey = n1.n_nationkey
    ->   and c_nationkey = n2.n_nationkey
    ->   and n1.n_name = 'FRANCE'
    ->   and n2.n_name = 'GERMANY'
    ->   and l_shipdate between date('1995-01-01') and date('1996-12-31');
+----+-------------+----------+------------+--------+----------------------------+--------------------+---------+-------------------------+------+----------+--------------------------------------------+
| id | select_type | table    | partitions | type   | possible_keys              | key                | key_len | ref                     | rows | filtered | Extra                                      |
+----+-------------+----------+------------+--------+----------------------------+--------------------+---------+-------------------------+------+----------+--------------------------------------------+
|  1 | SIMPLE      | n2       | NULL       | ALL    | PRIMARY                    | NULL               | NULL    | NULL                    |   25 |    10.00 | Using where                                |
|  1 | SIMPLE      | customer | NULL       | ref    | PRIMARY,fk_customer_nation | fk_customer_nation | 4       | tpch.n2.n_nationkey     | 6470 |   100.00 | Using index                                |
|  1 | SIMPLE      | orders   | NULL       | ref    | PRIMARY,fk_orders_customer | fk_orders_customer | 8       | tpch.customer.c_custkey |   15 |   100.00 | Using index                                |
|  1 | SIMPLE      | lineitem | NULL       | ref    | fk_lineitem_orders         | fk_lineitem_orders | 8       | tpch.orders.o_orderkey  |    3 |    11.11 | Using where                                |
|  1 | SIMPLE      | supplier | NULL       | eq_ref | PRIMARY,fk_supplier_nation | PRIMARY            | 8       | tpch.lineitem.l_suppkey |    1 |   100.00 | NULL                                       |
|  1 | SIMPLE      | n1       | NULL       | ALL    | PRIMARY                    | NULL               | NULL    | NULL                    |   25 |     4.00 | Using where; Using join buffer (hash join) |
+----+-------------+----------+------------+--------+----------------------------+--------------------+---------+-------------------------+------+----------+--------------------------------------------+
6 rows in set, 1 warning (0.00 sec)
 */
select
    n1.n_name as supp_nation,
    n2.n_name as cust_nation,
    extract(year from l_shipdate) as l_year,
    l_extendedprice * (1 - l_discount) as volume
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
  and l_shipdate between date('1995-01-01') and date('1996-12-31');

-- 7bW
/**
mysql> explain select
    ->     n1.n_name as supp_nation,
    ->     n2.n_name as cust_nation,
    ->     extract(year from l_shipdate) as l_year,
    ->     l_extendedprice * (1 - l_discount) as volume
    -> from
    ->     supplier,
    ->     lineitem,
    ->     orders,
    ->     customer,
    ->     nation n1,
    ->     nation n2
    -> where
    ->     s_suppkey = l_suppkey
    ->   and o_orderkey = l_orderkey
    ->   and c_custkey = o_custkey
    ->   and s_nationkey = n1.n_nationkey
    ->   and c_nationkey = n2.n_nationkey
    ->   and n1.n_name = 'GERMANY'
    ->   and n2.n_name = 'FRANCE'
    ->   and l_shipdate between date('1995-01-01') and date('1996-12-31');
+----+-------------+----------+------------+--------+----------------------------+--------------------+---------+-------------------------+------+----------+--------------------------------------------+
| id | select_type | table    | partitions | type   | possible_keys              | key                | key_len | ref                     | rows | filtered | Extra                                      |
+----+-------------+----------+------------+--------+----------------------------+--------------------+---------+-------------------------+------+----------+--------------------------------------------+
|  1 | SIMPLE      | n2       | NULL       | ALL    | PRIMARY                    | NULL               | NULL    | NULL                    |   25 |    10.00 | Using where                                |
|  1 | SIMPLE      | customer | NULL       | ref    | PRIMARY,fk_customer_nation | fk_customer_nation | 4       | tpch.n2.n_nationkey     | 6470 |   100.00 | Using index                                |
|  1 | SIMPLE      | orders   | NULL       | ref    | PRIMARY,fk_orders_customer | fk_orders_customer | 8       | tpch.customer.c_custkey |   15 |   100.00 | Using index                                |
|  1 | SIMPLE      | lineitem | NULL       | ref    | fk_lineitem_orders         | fk_lineitem_orders | 8       | tpch.orders.o_orderkey  |    3 |    11.11 | Using where                                |
|  1 | SIMPLE      | supplier | NULL       | eq_ref | PRIMARY,fk_supplier_nation | PRIMARY            | 8       | tpch.lineitem.l_suppkey |    1 |   100.00 | NULL                                       |
|  1 | SIMPLE      | n1       | NULL       | ALL    | PRIMARY                    | NULL               | NULL    | NULL                    |   25 |     4.00 | Using where; Using join buffer (hash join) |
+----+-------------+----------+------------+--------+----------------------------+--------------------+---------+-------------------------+------+----------+--------------------------------------------+
6 rows in set, 1 warning (0.00 sec)
 */
select
    n1.n_name as supp_nation,
    n2.n_name as cust_nation,
    extract(year from l_shipdate) as l_year,
    l_extendedprice * (1 - l_discount) as volume
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
  and n1.n_name = 'GERMANY'
  and n2.n_name = 'FRANCE'
  and l_shipdate between date('1995-01-01') and date('1996-12-31');

-- 8W
/**
mysql> explain select extract(year from o_orderdate) as o_year,     l_extendedprice * (1-l_discount) as volume,     n2.n_name as nation from     lineitem,     part,     supplier,     nation
n2,     orders,     customer,     nation n1,     region where p_partkey = l_partkey   and s_suppkey = l_suppkey   and l_orderkey = o_orderkey   and o_custkey = c_custkey   and c_nationkey =
n1.n_nationkey   and n1.n_regionkey = r_regionkey   and r_name = 'AMERICA'   and s_nationkey = n2.n_nationkey   and o_orderdate between date('1995-01-01') and date('1996-12-31')   and p_type = 'ECONOMY ANODIZED STEEL';
+----+-------------+----------+------------+--------+-----------------------------------------+--------------------+---------+---------------------------+---------+----------+--------------------------------------------+
| id | select_type | table    | partitions | type   | possible_keys                           | key                | key_len | ref                       | rows    | filtered | Extra                                      |
+----+-------------+----------+------------+--------+-----------------------------------------+--------------------+---------+---------------------------+---------+----------+--------------------------------------------+
|  1 | SIMPLE      | region   | NULL       | ALL    | PRIMARY                                 | NULL               | NULL    | NULL                      |       5 |    20.00 | Using where                                |
|  1 | SIMPLE      | n1       | NULL       | ref    | PRIMARY,fk_nation_region                | fk_nation_region   | 4       | tpch.region.r_regionkey   |       5 |   100.00 | Using index                                |
|  1 | SIMPLE      | orders   | NULL       | ALL    | PRIMARY,fk_orders_customer              | NULL               | NULL    | NULL                      | 1487306 |    11.11 | Using where; Using join buffer (hash join) |
|  1 | SIMPLE      | customer | NULL       | eq_ref | PRIMARY,fk_customer_nation              | PRIMARY            | 8       | tpch.orders.o_custkey     |       1 |     5.00 | Using where                                |
|  1 | SIMPLE      | lineitem | NULL       | ref    | fk_lineitem_orders,fk_lineitem_partsupp | fk_lineitem_orders | 8       | tpch.orders.o_orderkey    |       3 |   100.00 | NULL                                       |
|  1 | SIMPLE      | supplier | NULL       | eq_ref | PRIMARY,fk_supplier_nation              | PRIMARY            | 8       | tpch.lineitem.l_suppkey   |       1 |   100.00 | NULL                                       |
|  1 | SIMPLE      | n2       | NULL       | eq_ref | PRIMARY                                 | PRIMARY            | 4       | tpch.supplier.s_nationkey |       1 |   100.00 | NULL                                       |
|  1 | SIMPLE      | part     | NULL       | eq_ref | PRIMARY                                 | PRIMARY            | 8       | tpch.lineitem.l_partkey   |       1 |    10.00 | Using where                                |
+----+-------------+----------+------------+--------+-----------------------------------------+--------------------+---------+---------------------------+---------+----------+--------------------------------------------+
8 rows in set, 1 warning (0.00 sec)
 */
select
    extract(year from o_orderdate) as o_year,
    l_extendedprice * (1-l_discount) as volume,
    n2.n_name as nation
from
    part,
    supplier,
    lineitem,
    orders,
    customer,
    nation n1,
    nation n2,
    region
where p_partkey = l_partkey
  and s_suppkey = l_suppkey
  and l_orderkey = o_orderkey
  and o_custkey = c_custkey
  and c_nationkey = n1.n_nationkey
  and n1.n_regionkey = r_regionkey
  and r_name = 'AMERICA'
  and s_nationkey = n2.n_nationkey
  and o_orderdate between date('1995-01-01') and date('1996-12-31')
  and p_type = 'ECONOMY ANODIZED STEEL';


/**
  mysql> explain select
    ->     /*+ JOIN_PREFIX(lineitem) */
    ->     extract(year from o_orderdate) as o_year,
    ->     l_extendedprice * (1-l_discount) as volume,
    ->     n2.n_name as nation
    -> from
    ->     part,
    ->     supplier,
    ->     lineitem,
    ->     orders,
    ->     customer,
    ->     nation n1,
    ->     nation n2,
    ->     region
    -> where p_partkey = l_partkey
    ->   and s_suppkey = l_suppkey
    ->   and l_orderkey = o_orderkey
    ->   and o_custkey = c_custkey
    ->   and c_nationkey = n1.n_nationkey
    ->   and n1.n_regionkey = r_regionkey
    ->   and r_name = 'AMERICA'
    ->   and s_nationkey = n2.n_nationkey
    ->   and o_orderdate between date('1995-01-01') and date('1996-12-31')
    ->   and p_type = 'ECONOMY ANODIZED STEEL';
+----+-------------+----------+------------+--------+-----------------------------------------+---------+---------+---------------------------+---------+----------+--------------------------------------------+
| id | select_type | table    | partitions | type   | possible_keys                           | key     | key_len | ref                       | rows    | filtered | Extra                                      |
+----+-------------+----------+------------+--------+-----------------------------------------+---------+---------+---------------------------+---------+----------+--------------------------------------------+
|  1 | SIMPLE      | lineitem | NULL       | ALL    | fk_lineitem_orders,fk_lineitem_partsupp | NULL    | NULL    | NULL                      | 5782325 |   100.00 | NULL                                       |
|  1 | SIMPLE      | supplier | NULL       | eq_ref | PRIMARY,fk_supplier_nation              | PRIMARY | 8       | tpch.lineitem.l_suppkey   |       1 |   100.00 | NULL                                       |
|  1 | SIMPLE      | part     | NULL       | eq_ref | PRIMARY                                 | PRIMARY | 8       | tpch.lineitem.l_partkey   |       1 |    10.00 | Using where                                |
|  1 | SIMPLE      | n2       | NULL       | eq_ref | PRIMARY                                 | PRIMARY | 4       | tpch.supplier.s_nationkey |       1 |   100.00 | NULL                                       |
|  1 | SIMPLE      | orders   | NULL       | eq_ref | PRIMARY,fk_orders_customer              | PRIMARY | 8       | tpch.lineitem.l_orderkey  |       1 |    11.11 | Using where                                |
|  1 | SIMPLE      | region   | NULL       | ALL    | PRIMARY                                 | NULL    | NULL    | NULL                      |       5 |    20.00 | Using where; Using join buffer (hash join) |
|  1 | SIMPLE      | customer | NULL       | eq_ref | PRIMARY,fk_customer_nation              | PRIMARY | 8       | tpch.orders.o_custkey     |       1 |   100.00 | NULL                                       |
|  1 | SIMPLE      | n1       | NULL       | eq_ref | PRIMARY,fk_nation_region                | PRIMARY | 4       | tpch.customer.c_nationkey |       1 |    20.00 | Using where                                |
+----+-------------+----------+------------+--------+-----------------------------------------+---------+---------+---------------------------+---------+----------+--------------------------------------------+
8 rows in set, 1 warning (0.00 sec)

Note this plan doesn't work because join between orders and region is a cross-product. In a way, to avoid cross-product,
customer > n1 > region where ">" means come before.
 */
select
    /*+ JOIN_PREFIX(lineitem) */
    extract(year from o_orderdate) as o_year,
    l_extendedprice * (1-l_discount) as volume,
    n2.n_name as nation
from
    part,
    supplier,
    lineitem,
    orders,
    customer,
    nation n1,
    nation n2,
    region
where p_partkey = l_partkey
  and s_suppkey = l_suppkey
  and l_orderkey = o_orderkey
  and o_custkey = c_custkey
  and c_nationkey = n1.n_nationkey
  and n1.n_regionkey = r_regionkey
  and r_name = 'AMERICA'
  and s_nationkey = n2.n_nationkey
  and o_orderdate between date('1995-01-01') and date('1996-12-31')
  and p_type = 'ECONOMY ANODIZED STEEL';