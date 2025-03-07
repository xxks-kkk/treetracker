-- setup Postgres database schemas for TPC-H
-- instruction: psql -p5432 -d tpch -f /home/zeyuanhu/projects/treetracker2/treeTracker/treetracker-benchmark/src/main/resources/tpch-original.sql
BEGIN TRANSACTION;

DROP TABLE IF EXISTS part, supplier, partsupp, customer, orders, lineitem, nation, region;

CREATE UNLOGGED TABLE part
(
    P_PARTKEY     INTEGER,
    P_NAME        VARCHAR,
    P_MFGR        VARCHAR,
    P_BRAND       VARCHAR,
    P_TYPE        VARCHAR,
    P_SIZE        INTEGER,
    P_CONTAINER   VARCHAR,
    P_RETAILPRICE DOUBLE PRECISION,
    P_COMMENT     VARCHAR
);

CREATE UNLOGGED TABLE supplier
(
    s_suppkey   INTEGER,
    S_NAME      VARCHAR,
    S_ADDRESS   VARCHAR,
    s_nationkey INTEGER, -- references N_NATIONKEY
    S_PHONE     VARCHAR,
    S_ACCTBAL   DOUBLE PRECISION,
    S_COMMENT   VARCHAR
);

CREATE UNLOGGED TABLE partsupp
(
    ps_partkey     INTEGER, -- references P_PARTKEY
    ps_suppkey     INTEGER, -- references S_SUPPKEY
    PS_AVAILQTY   INTEGER,
    PS_SUPPLYCOST DOUBLE PRECISION,
    PS_COMMENT    VARCHAR
);

CREATE UNLOGGED TABLE customer
(
    c_custkey    INTEGER,
    C_NAME       VARCHAR,
    C_ADDRESS    VARCHAR,
    c_nationkey  INTEGER, -- references N_NATIONKEY
    C_PHONE      VARCHAR,
    C_ACCTBAL    DOUBLE PRECISION,
    C_MKTSEGMENT VARCHAR,
    C_COMMENT    VARCHAR
);

CREATE UNLOGGED TABLE orders
(
    O_orderkey      INTEGER,
    O_custkey       INTEGER, -- references C_CUSTKEY
    O_ORDERSTATUS   VARCHAR,
    O_TOTALPRICE    DOUBLE PRECISION,
    O_ORDERDATE     DATE,
    O_ORDERPRIORITY VARCHAR,
    O_CLERK         VARCHAR,
    O_SHIPPRIORITY  INTEGER,
    O_COMMENT       VARCHAR
);

CREATE UNLOGGED TABLE lineitem
(
    L_orderkey      INTEGER, -- references O_ORDERKEY
    L_partkey       INTEGER, -- references P_PARTKEY (compound fk to PARTSUPP)
    L_suppkey       INTEGER, -- references S_SUPPKEY (compound fk to PARTSUPP)
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
    L_COMMENT       VARCHAR
);

CREATE UNLOGGED TABLE nation
(
    N_nationkey INTEGER,
    N_NAME      VARCHAR,
    N_regionkey INTEGER, -- references R_REGIONKEY
    N_COMMENT   VARCHAR
);

CREATE UNLOGGED TABLE region
(
    R_regionkey INTEGER,
    R_NAME      VARCHAR,
    R_COMMENT   VARCHAR
);

COPY part FROM '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s1/part.tbl' WITH DELIMITER '|' NULL '\N' CSV;
COPY supplier FROM '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s1/supplier.tbl' WITH DELIMITER '|' NULL '\N' CSV;
COPY partsupp FROM '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s1/partsupp.tbl' WITH DELIMITER '|' NULL '\N' CSV;
COPY customer FROM '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s1/customer.tbl' WITH DELIMITER '|' NULL '\N' CSV;
COPY orders FROM '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s1/orders.tbl' WITH DELIMITER '|' NULL '\N' CSV;
COPY lineitem FROM '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s1/lineitem.tbl' WITH DELIMITER '|' NULL '\N' CSV;
COPY nation FROM '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s1/nation.tbl' WITH DELIMITER '|' NULL '\N' CSV;
COPY region FROM '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s1/region.tbl' WITH DELIMITER '|' NULL '\N' CSV;

COMMIT;

ALTER TABLE part
    SET LOGGED;
ALTER TABLE supplier
    SET LOGGED;
ALTER TABLE partsupp
    SET LOGGED;
ALTER TABLE customer
    SET LOGGED;
ALTER TABLE orders
    SET LOGGED;
ALTER TABLE lineitem
    SET LOGGED;
ALTER TABLE nation
    SET LOGGED;
ALTER TABLE region
    SET LOGGED;

-- For 15W
create view lineitemrevenue (supplier_no, total_revenue) as
with a1 as (select l_suppkey,
                   sum(l_extendedprice * (1 - l_discount)) as total_revenue
            from lineitem
            where l_shipdate >= date '1996-01-01'
              and l_shipdate < date '1996-01-01' + interval '3' month
            group by l_suppkey)
SELECT l_suppkey, total_revenue
FROM a1
WHERE total_revenue = (SELECT MAX(total_revenue) FROM a1);