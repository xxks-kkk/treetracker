-- setup Postgres database schemas for TPC-H
-- instruction: psql -p5432 -d postgres -f treeTracker/treetracker-benchmark/src/main/resources/tpch-s10.sql
BEGIN TRANSACTION;

DROP SCHEMA IF EXISTS tpch CASCADE;
CREATE SCHEMA tpch;

CREATE UNLOGGED TABLE TPCH.part
(
    PARTKEY     INTEGER,
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
    SUPPKEY   INTEGER,
    S_NAME      VARCHAR,
    S_ADDRESS   VARCHAR,
    NATIONKEY INTEGER, -- references N_NATIONKEY
    S_PHONE     VARCHAR,
    S_ACCTBAL   DOUBLE PRECISION,
    S_COMMENT   VARCHAR
);

CREATE UNLOGGED TABLE TPCH.partsupp
(
    PARTKEY     INTEGER, -- references P_PARTKEY
    SUPPKEY     INTEGER, -- references S_SUPPKEY
    PS_AVAILQTY   INTEGER,
    PS_SUPPLYCOST DOUBLE PRECISION,
    PS_COMMENT    VARCHAR
);

CREATE UNLOGGED TABLE TPCH.customer
(
    CUSTKEY    INTEGER,
    C_NAME       VARCHAR,
    C_ADDRESS    VARCHAR,
    NATIONKEY  INTEGER, -- references N_NATIONKEY
    C_PHONE      VARCHAR,
    C_ACCTBAL    DOUBLE PRECISION,
    C_MKTSEGMENT VARCHAR,
    C_COMMENT    VARCHAR
);

CREATE UNLOGGED TABLE TPCH.orders
(
    ORDERKEY      INTEGER,
    CUSTKEY       INTEGER, -- references C_CUSTKEY
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
    ORDERKEY      INTEGER, -- references O_ORDERKEY
    PARTKEY       INTEGER, -- references P_PARTKEY (compound fk to PARTSUPP)
    SUPPKEY       INTEGER, -- references S_SUPPKEY (compound fk to PARTSUPP)
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
    NATIONKEY INTEGER,
    N_NAME      VARCHAR,
    REGIONKEY INTEGER, -- references R_REGIONKEY
    N_COMMENT   VARCHAR
);

CREATE UNLOGGED TABLE TPCH.region
(
    REGIONKEY INTEGER,
    R_NAME      VARCHAR,
    R_COMMENT   VARCHAR
);

COPY TPCH.part FROM '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s10/part.tbl' WITH DELIMITER '|' NULL '\N' CSV;
COPY TPCH.supplier FROM '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s10/supplier.tbl' WITH DELIMITER '|' NULL '\N' CSV;
COPY TPCH.partsupp FROM '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s10/partsupp.tbl' WITH DELIMITER '|' NULL '\N' CSV;
COPY TPCH.customer FROM '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s10/customer.tbl' WITH DELIMITER '|' NULL '\N' CSV;
COPY TPCH.orders FROM '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s10/orders.tbl' WITH DELIMITER '|' NULL '\N' CSV;
COPY TPCH.lineitem FROM '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s10/lineitem.tbl' WITH DELIMITER '|' NULL '\N' CSV;
COPY TPCH.nation FROM '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s10/nation.tbl' WITH DELIMITER '|' NULL '\N' CSV;
COPY TPCH.region FROM '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s10/region.tbl' WITH DELIMITER '|' NULL '\N' CSV;

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