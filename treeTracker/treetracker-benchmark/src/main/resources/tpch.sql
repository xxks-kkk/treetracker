-- setup Postgres database schemas for TPC-H
-- instruction: psql -p5432 -d postgres -f /home/zeyuanhu/projects/treetracker2/treeTracker/treetracker-benchmark/src/main/resources/tpch.sql
BEGIN TRANSACTION;

DROP SCHEMA IF EXISTS tpch CASCADE;
CREATE SCHEMA tpch;

CREATE UNLOGGED TABLE TPCH.part
(
    partkey     INTEGER,
    P_NAME        VARCHAR,
    P_MFGR        VARCHAR,
    P_BRAND       VARCHAR,
    P_TYPE        VARCHAR,
    P_SIZE        INTEGER,
    P_CONTAINER   VARCHAR,
    P_RETAILPRICE DOUBLE PRECISION,
    P_COMMENT     VARCHAR
);

CREATE UNLOGGED TABLE TPCH.supplier
(
    suppkey   INTEGER,
    S_NAME      VARCHAR,
    S_ADDRESS   VARCHAR,
    nationkey INTEGER, -- references N_NATIONKEY
    S_PHONE     VARCHAR,
    S_ACCTBAL   DOUBLE PRECISION,
    S_COMMENT   VARCHAR
);

CREATE UNLOGGED TABLE TPCH.partsupp
(
    partkey     INTEGER, -- references P_PARTKEY
    suppkey     INTEGER, -- references S_SUPPKEY
    PS_AVAILQTY   INTEGER,
    PS_SUPPLYCOST DOUBLE PRECISION,
    PS_COMMENT    VARCHAR
);

CREATE UNLOGGED TABLE TPCH.customer
(
    custkey    INTEGER,
    C_NAME       VARCHAR,
    C_ADDRESS    VARCHAR,
    nationkey  INTEGER, -- references N_NATIONKEY
    C_PHONE      VARCHAR,
    C_ACCTBAL    DOUBLE PRECISION,
    C_MKTSEGMENT VARCHAR,
    C_COMMENT    VARCHAR
);

CREATE UNLOGGED TABLE TPCH.orders
(
    orderkey      INTEGER,
    custkey       INTEGER, -- references C_CUSTKEY
    O_ORDERSTATUS   VARCHAR,
    O_TOTALPRICE    DOUBLE PRECISION,
    O_ORDERDATE     DATE,
    O_ORDERPRIORITY VARCHAR,
    O_CLERK         VARCHAR,
    O_SHIPPRIORITY  INTEGER,
    O_COMMENT       VARCHAR
);

CREATE UNLOGGED TABLE TPCH.lineitem
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
    L_COMMENT       VARCHAR
);

CREATE UNLOGGED TABLE TPCH.nation
(
    nationkey INTEGER,
    N_NAME      VARCHAR,
    regionkey INTEGER, -- references R_REGIONKEY
    N_COMMENT   VARCHAR
);

CREATE UNLOGGED TABLE TPCH.region
(
    regionkey INTEGER,
    R_NAME      VARCHAR,
    R_COMMENT   VARCHAR
);

COPY TPCH.part FROM '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s1/part.tbl' WITH DELIMITER '|' NULL '\N' CSV;
COPY TPCH.supplier FROM '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s1/supplier.tbl' WITH DELIMITER '|' NULL '\N' CSV;
COPY TPCH.partsupp FROM '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s1/partsupp.tbl' WITH DELIMITER '|' NULL '\N' CSV;
COPY TPCH.customer FROM '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s1/customer.tbl' WITH DELIMITER '|' NULL '\N' CSV;
COPY TPCH.orders FROM '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s1/orders.tbl' WITH DELIMITER '|' NULL '\N' CSV;
COPY TPCH.lineitem FROM '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s1/lineitem.tbl' WITH DELIMITER '|' NULL '\N' CSV;
COPY TPCH.nation FROM '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s1/nation.tbl' WITH DELIMITER '|' NULL '\N' CSV;
COPY TPCH.region FROM '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s1/region.tbl' WITH DELIMITER '|' NULL '\N' CSV;

COMMIT;

ALTER TABLE TPCH.part
    SET LOGGED;
ALTER TABLE TPCH.supplier
    SET LOGGED;
ALTER TABLE TPCH.partsupp
    SET LOGGED;
ALTER TABLE TPCH.customer
    SET LOGGED;
ALTER TABLE TPCH.orders
    SET LOGGED;
ALTER TABLE TPCH.lineitem
    SET LOGGED;
ALTER TABLE TPCH.nation
    SET LOGGED;
ALTER TABLE TPCH.region
    SET LOGGED;

-- CREATE INDEX orderdate_idx
--     ON tpch.orders(o_orderdate);