/*
    setup duckdb database schemas of TPC-H

    This script, we rename the attributes to make them form natural join and drop redundant columns

    duckdb /home/zeyuanhu/projects/treetracker2/treeTracker/tpch-s1.duckdb < /home/zeyuanhu/projects/treetracker2/treeTracker/treetracker-benchmark/src/main/resources/tpch-abbreviated-duckdb.sql
 */
BEGIN TRANSACTION;

DROP SCHEMA IF EXISTS tpch_int CASCADE;
CREATE SCHEMA tpch_int;

CREATE TABLE tpch_int.nation_tmp
(
    nationkey INTEGER not null,
    n_name    varchar not null,
    regionkey INTEGER not null,
    n_comment varchar
);

CREATE TABLE tpch_int.region_tmp
(
    regionkey INTEGER not null,
    r_name    varchar not null,
    r_comment varchar
);

CREATE TABLE tpch_int.part_tmp
(
    partkey       integer not null,
    p_name        VARCHAR not null,
    p_mfgr        varchar not null,
    p_brand       varchar not null,
    p_type        varchar not null,
    p_size        INTEGER not null,
    p_container   varchar not null,
    p_retailprice varchar not null,
    p_comment     varchar not null
);

CREATE TABLE tpch_int.supplier_tmp
(
    suppkey   integer not null,
    s_name    varchar not null,
    s_address varchar not null,
    nationkey INTEGER not null,
    s_phone   varchar not null,
    s_acctbal varchar not null,
    s_comment varchar not null
);

CREATE TABLE tpch_int.partsupp_tmp
(
    partkey       integer not null,
    suppkey       integer not null,
    ps_availqty   integer not null,
    ps_supplycost varchar not null,
    ps_comment    VARCHAR not null
);

CREATE TABLE tpch_int.customer_tmp
(
    custkey      integer not null,
    c_name       varchar not null,
    c_address    varchar not null,
    nationkey    INTEGER not null,
    c_phone      varchar not null,
    c_acctbal    varchar not null,
    c_mktsegment varchar not null,
    c_comment    varchar not null
);

CREATE TABLE tpch_int.orders_tmp
(
    orderkey        integer not null,
    custkey         integer not null,
    o_orderstatus   varchar not null,
    o_totalprice    varchar not null,
    o_orderdate     varchar not null,
    o_orderpriority varchar not null,
    o_clerk         varchar not null,
    o_shippriority  INTEGER not null,
    o_comment       varchar not null
);

CREATE TABLE tpch_int.lineitem_tmp
(
    orderkey        integer          not null,
    partkey         integer          not null,
    suppkey         integer          not null,
    l_linenumber    BIGINT           not null,
    l_quantity      DOUBLE PRECISION not null,
    l_extendedprice DOUBLE PRECISION not null,
    l_discount      DOUBLE PRECISION not null,
    l_tax           DOUBLE PRECISION not null,
    l_returnflag    varchar          not null,
    l_linestatus    varchar          not null,
    l_shipdate      DATE             not null,
    l_commitdate    DATE             not null,
    l_receiptdate   DATE             not null,
    l_shipinstruct  CHAR(25)         not null,
    l_shipmode      CHAR(10)         not null,
    l_comment       varchar          not null
);

COPY tpch_int.nation_tmp FROM '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s1/nation.tbl' WITH DELIMITER '|' NULL '\N' CSV;
COPY tpch_int.customer_tmp FROM '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s1/customer.tbl' WITH DELIMITER '|' NULL '\N' CSV;
COPY tpch_int.lineitem_tmp FROM '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s1/lineitem.tbl' WITH DELIMITER '|' NULL '\N' CSV;
COPY tpch_int.orders_tmp FROM '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s1/orders.tbl' WITH DELIMITER '|' NULL '\N' CSV;
COPY tpch_int.part_tmp FROM '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s1/part.tbl' WITH DELIMITER '|' NULL '\N' CSV;
COPY tpch_int.partsupp_tmp FROM '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s1/partsupp.tbl' WITH DELIMITER '|' NULL '\N' CSV;
COPY tpch_int.region_tmp FROM '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s1/region.tbl' WITH DELIMITER '|' NULL '\N' CSV;
COPY tpch_int.supplier_tmp FROM '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s1/supplier.tbl' WITH DELIMITER '|' NULL '\N' CSV;

CREATE TABLE tpch_int.region
(
    regionkey INTEGER primary key,
);

CREATE TABLE tpch_int.nation
(
    nationkey INTEGER primary key,
    regionkey INTEGER, -- references R_REGIONKEY
    FOREIGN KEY (regionkey) REFERENCES tpch_int.region(regionkey)
);

CREATE TABLE tpch_int.part
(
    partkey     INTEGER primary key
);

CREATE TABLE tpch_int.supplier
(
    suppkey   INTEGER primary key,
    nationkey INTEGER, -- references N_NATIONKEY
    FOREIGN KEY (nationkey) REFERENCES tpch_int.nation(nationkey)
);

CREATE TABLE tpch_int.partsupp
(
    partkey     INTEGER, -- references P_PARTKEY
    suppkey     INTEGER, -- references S_SUPPKEY
    FOREIGN KEY (partkey) REFERENCES tpch_int.part(partkey),
    FOREIGN KEY (suppkey) REFERENCES tpch_int.supplier(suppkey),
    UNIQUE (partkey, suppkey)
);

CREATE TABLE tpch_int.customer
(
    custkey    INTEGER primary key,
    nationkey  INTEGER, -- references N_NATIONKEY
    FOREIGN KEY (nationkey) REFERENCES tpch_int.nation(nationkey)
);

CREATE TABLE tpch_int.orders
(
    orderkey      INTEGER primary key,
    custkey       INTEGER, -- references C_CUSTKEY
    FOREIGN KEY (custkey) REFERENCES tpch_int.customer(custkey)
);

CREATE TABLE tpch_int.lineitem
(
    orderkey      INTEGER, -- references O_ORDERKEY
    partkey       INTEGER, -- references P_PARTKEY (compound fk to PARTSUPP)
    suppkey       INTEGER, -- references S_SUPPKEY (compound fk to PARTSUPP)
    FOREIGN KEY (orderkey) REFERENCES tpch_int.orders(orderkey),
    FOREIGN KEY (partkey, suppkey) REFERENCES tpch_int.partsupp(partkey, suppkey)
);

INSERT INTO tpch_int.region SELECT regionkey from tpch_int.region_tmp;
INSERT INTO tpch_int.nation SELECT nationkey, regionkey from tpch_int.nation_tmp;
INSERT INTO tpch_int.part SELECT partkey from tpch_int.part_tmp;
INSERT INTO tpch_int.supplier SELECT suppkey, nationkey from tpch_int.supplier_tmp;
INSERT INTO tpch_int.partsupp SELECT partkey, suppkey from tpch_int.partsupp_tmp;
INSERT INTO tpch_int.customer SELECT custkey, nationkey from tpch_int.customer_tmp;
INSERT INTO tpch_int.orders SELECT orderkey, custkey from tpch_int.orders_tmp;
INSERT INTO tpch_int.lineitem SELECT orderkey, partkey, suppkey from tpch_int.lineitem_tmp;

DROP TABLE tpch_int.nation_tmp;
DROP TABLE tpch_int.region_tmp;
DROP TABLE tpch_int.part_tmp;
DROP TABLE tpch_int.supplier_tmp;
DROP TABLE tpch_int.partsupp_tmp;
DROP TABLE tpch_int.customer_tmp;
DROP TABLE tpch_int.orders_tmp;
DROP TABLE tpch_int.lineitem_tmp;

COMMIT;

analyze tpch_int.part;
analyze tpch_int.supplier;
analyze tpch_int.partsupp;
analyze tpch_int.customer;
analyze tpch_int.orders;
analyze tpch_int.lineitem;
analyze tpch_int.nation;
analyze tpch_int.region;