/*
    setup duckdb database schemas for TPC-H

    instruction:
    duckdb /home/zeyuanhu/projects/treetracker2/treeTracker/tpch-s1.duckdb < /home/zeyuanhu/projects/treetracker2/treeTracker/treetracker-benchmark/src/main/resources/tpch-duckdb.sql
 */
BEGIN TRANSACTION;

DROP SCHEMA IF EXISTS tpch CASCADE;
CREATE SCHEMA tpch;

CREATE TABLE TPCH.region
(
    regionkey INTEGER primary key,
    R_NAME      VARCHAR,
    R_COMMENT   VARCHAR
);

CREATE TABLE TPCH.nation
(
    nationkey INTEGER primary key,
    N_NAME      VARCHAR,
    regionkey INTEGER, -- references R_REGIONKEY
    N_COMMENT   VARCHAR,
    FOREIGN KEY (regionkey) REFERENCES TPCH.region(regionkey)
);

CREATE TABLE TPCH.part
(
    partkey     INTEGER primary key,
    P_NAME        VARCHAR,
    P_MFGR        VARCHAR,
    P_BRAND       VARCHAR,
    P_TYPE        VARCHAR,
    P_SIZE        INTEGER,
    P_CONTAINER   VARCHAR,
    P_RETAILPRICE DOUBLE PRECISION,
    P_COMMENT     VARCHAR
);

CREATE TABLE TPCH.supplier
(
    suppkey   INTEGER primary key,
    S_NAME      VARCHAR,
    S_ADDRESS   VARCHAR,
    nationkey INTEGER, -- references N_NATIONKEY
    S_PHONE     VARCHAR,
    S_ACCTBAL   DOUBLE PRECISION,
    S_COMMENT   VARCHAR,
    FOREIGN KEY (nationkey) REFERENCES TPCH.nation(nationkey)
);

CREATE TABLE TPCH.partsupp
(
    partkey     INTEGER, -- references P_PARTKEY
    suppkey     INTEGER, -- references S_SUPPKEY
    PS_AVAILQTY   INTEGER,
    PS_SUPPLYCOST DOUBLE PRECISION,
    PS_COMMENT    VARCHAR,
    FOREIGN KEY (partkey) REFERENCES TPCH.part(partkey),
    FOREIGN KEY (suppkey) REFERENCES TPCH.supplier(suppkey),
    UNIQUE (partkey, suppkey)
);

CREATE TABLE TPCH.customer
(
    custkey    INTEGER primary key,
    C_NAME       VARCHAR,
    C_ADDRESS    VARCHAR,
    nationkey  INTEGER, -- references N_NATIONKEY
    C_PHONE      VARCHAR,
    C_ACCTBAL    DOUBLE PRECISION,
    C_MKTSEGMENT VARCHAR,
    C_COMMENT    VARCHAR,
    FOREIGN KEY (nationkey) REFERENCES TPCH.nation(nationkey)
);

CREATE TABLE TPCH.orders
(
    orderkey      INTEGER primary key,
    custkey       INTEGER, -- references C_CUSTKEY
    O_ORDERSTATUS   VARCHAR,
    O_TOTALPRICE    DOUBLE PRECISION,
    O_ORDERDATE     DATE,
    O_ORDERPRIORITY VARCHAR,
    O_CLERK         VARCHAR,
    O_SHIPPRIORITY  INTEGER,
    O_COMMENT       VARCHAR,
    FOREIGN KEY (custkey) REFERENCES TPCH.customer(custkey)
);

CREATE TABLE TPCH.lineitem
(
    orderkey      INTEGER, -- references O_ORDERKEY
    partkey       INTEGER, -- references P_PARTKEY (compound fk to PARTSUPP)
    suppkey       INTEGER, -- references S_SUPPKEY (compound fk to PARTSUPP)
    L_LINENUMBER    INTEGER,
    L_QUANTITY      DOUBLE PRECISION,
    L_EXTENDEDPRICE DOUBLE PRECISION,
    L_DISCOUNT      DOUBLE PRECISION,
    L_TAX           DOUBLE PRECISION,
    L_RETURNFLAG    VARCHAR,
    L_LINESTATUS    VARCHAR,
    L_SHIPDATE      DATE,
    L_COMMITDATE    DATE,
    L_RECEIPTDATE   DATE,
    L_SHIPINSTRUCT  VARCHAR,
    L_SHIPMODE      VARCHAR,
    L_COMMENT       VARCHAR,
    FOREIGN KEY (orderkey) REFERENCES TPCH.orders(orderkey),
    FOREIGN KEY (partkey, suppkey) REFERENCES TPCH.partsupp(partkey, suppkey)
);

COPY TPCH.region FROM '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s1/region.tbl' WITH DELIMITER '|' NULL '\N' CSV;
COPY TPCH.nation FROM '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s1/nation.tbl' WITH DELIMITER '|' NULL '\N' CSV;
COPY TPCH.part FROM '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s1/part.tbl' WITH DELIMITER '|' NULL '\N' CSV;
COPY TPCH.supplier FROM '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s1/supplier.tbl' WITH DELIMITER '|' NULL '\N' CSV;
COPY TPCH.partsupp FROM '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s1/partsupp.tbl' WITH DELIMITER '|' NULL '\N' CSV;
COPY TPCH.customer FROM '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s1/customer.tbl' WITH DELIMITER '|' NULL '\N' CSV;
COPY TPCH.orders FROM '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s1/orders.tbl' WITH DELIMITER '|' NULL '\N' CSV;
COPY TPCH.lineitem FROM '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s1/lineitem.tbl' WITH DELIMITER '|' NULL '\N' CSV;

COMMIT;

analyze TPCH.part;
analyze TPCH.supplier;
analyze TPCH.partsupp;
analyze TPCH.customer;
analyze TPCH.orders;
analyze TPCH.lineitem;
analyze TPCH.nation;
analyze TPCH.region;