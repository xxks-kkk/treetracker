/*
 Q1

 Q1 doesn't contain join --> skip.
 */

/*
  Q2
  Expectation: hash join = TTJ > LIP > Yannakakis
 */
-- check join result of Q2
select * from tpch."PARTSUPP" natural join
    tpch."PART" natural join
    tpch."SUPPLIER" natural join
    tpch."NATION" natural join
    tpch."REGION";

-- find if there is any tuple in PARTSUPP that is not joinable with PART or SUPPLIER
-- res: 0
select p_partkey
from tpch."PARTSUPP"
where p_partkey not in (select p_partkey from tpch."PART")
union
select s_suppkey
from tpch."PARTSUPP"
where s_suppkey not in (select "SUPPLIER".s_suppkey from tpch."SUPPLIER");

-- find if there is any tuple in SUPPLIER that is not joinable with NATION
-- res: 0
select n_nationkey
from tpch."SUPPLIER"
where n_nationkey not in (select n_nationkey from tpch."NATION");

-- find if there is any tuple in NATION that is not joinable with REGION
-- res: 0
select r_regionkey
from tpch."NATION"
where r_regionkey not in (select r_regionkey from tpch."REGION");

/*
  Q3
  Expectation: hash join = TTJ > LIP > Yannakakis
 */
-- check join result of Q3
select * from tpch."CUSTOMER" natural join tpch."ORDERS" natural join tpch."LINEITEM";

-- check whether there is any no-good tuples from CUSTOMER w.r.t ORDER
-- res: 50004
select count(c_custkey)
from tpch."CUSTOMER"
where c_custkey not in (select c_custkey from tpch."ORDERS");

-- check uniqueness on no-good tuples from CUSTOMER w.r.t ORDER
-- res: 50004
select count(distinct c_custkey)
from tpch."CUSTOMER"
where c_custkey not in (select c_custkey from tpch."ORDERS");

-- find if there is any no-good tuple in ORDER w.r.t CUSTOMER
-- res: 0
select count(c_custkey)
from tpch."ORDERS"
where c_custkey not in (select c_custkey from tpch."CUSTOMER");

-- find if there is any no-good tuple in LINEITEM w.r.t ORDER
-- res: 0
select count(o_orderkey)
from tpch."LINEITEM"
where o_orderkey not in (select "ORDERS".o_orderkey from tpch."ORDERS");

-- find if there is any no-good tuple in ORDER w.r.t LINEITEM
-- res: 0
select count(o_orderkey)
from tpch."ORDERS"
where o_orderkey not in (select "LINEITEM".o_orderkey from tpch."LINEITEM");