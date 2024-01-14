-- setup Postgres database schemas for Star Schema Benchmarok (SSB)
-- instruction: psql -p5432 -d ssb -f ssb.sql
-- same as ssb.sql but used to experiment DDLs to improve bulk loading speed and potential to
-- reduce memory footprint of TTJ benchmarks (with the goal to improve TTJ benchmarking performance)
BEGIN TRANSACTION;

DROP SCHEMA IF EXISTS ssb CASCADE;
CREATE SCHEMA ssb;

CREATE UNLOGGED TABLE SSB."CUSTOMER" (CUSTKEY     varchar,
                           C_NAME        varchar,
                           C_ADDRESS     varchar,
                           C_CITY        varchar,
                           C_NATION      varchar,
                           C_REGION      varchar,
                           C_PHONE       varchar,
                           C_MKTSEGMENT  varchar);

CREATE UNLOGGED TABLE SSB."LINEORDER" (
    LO_ORDERKEY             varchar,
    LO_LINENUMBER           varchar,
    CUSTKEY              varchar,
    PARTKEY              varchar,
    SUPPKEY              varchar,
    DATEKEY            varchar,
    LO_ORDERPRIORITY        varchar,
    LO_SHIPPRIORITY         varchar,
    LO_QUANTITY             varchar,
    LO_EXTENDEDPRICE        varchar,
    LO_ORDTOTALPRICE        varchar,
    LO_DISCOUNT             varchar,
    LO_REVENUE              varchar,
    LO_SUPPLYCOST           varchar,
    LO_TAX                  varchar,
    LO_COMMITDATE           varchar,
    LO_SHIPMODE             varchar);

CREATE UNLOGGED TABLE SSB."PART" (PARTKEY     varchar,
                       P_NAME        varchar,
                       P_MFGR        varchar,
                       P_CATEGORY    varchar,
                       P_BRAND1      varchar,
                       P_COLOR       varchar,
                       P_TYPE        varchar,
                       P_SIZE        varchar,
                       P_CONTAINER   varchar);

CREATE UNLOGGED TABLE SSB."SUPPLIER" (SUPPKEY     varchar,
                            S_NAME        varchar,
                            S_ADDRESS     varchar,
                            S_CITY        varchar,
                            S_NATION      varchar,
                            S_REGION      varchar,
                            S_PHONE       varchar);

CREATE UNLOGGED TABLE SSB."DATE"
(
  DATEKEY      varchar,
  D_DATE         varchar,
  D_DAYOFWEEK    varchar,
  D_MONTH       varchar,
  D_YEAR         varchar,
  D_YEARMONTHNUM varchar,
  D_YEARMONTH    varchar,
  D_DAYNUMINWEEK varchar,
  D_DAYNUMINMONTH varchar,
  D_DAYNUMINYEAR varchar,
  D_MONTHNUMINYEAR varchar,
  D_WEEKNUMINYEAR varchar,
  D_SELLINGSEASON varchar,
  D_LASTDAYINWEEKFL varchar,
  D_LASTDAYINMONTHFL varchar,
  D_HOLIDAYFL varchar,
  D_WEEKDAYFL varchar);

-- COPY SSB."CUSTOMER" FROM '/home/zeyuanhu/projects/eyalroz-ssb-dbgen/postgres/s1/customer.tbl' WITH DELIMITER '|' NULL '\N'  CSV;
-- COPY SSB."LINEORDER" FROM '/home/zeyuanhu/projects/eyalroz-ssb-dbgen/postgres/s1/lineorder.tbl' WITH DELIMITER '|' NULL '\N'  CSV;
-- COPY SSB."PART" FROM '/home/zeyuanhu/projects/eyalroz-ssb-dbgen/postgres/s1/part.tbl' WITH DELIMITER '|' NULL '\N'  CSV;
-- COPY SSB."SUPPLIER" FROM '/home/zeyuanhu/projects/eyalroz-ssb-dbgen/postgres/s1/supplier.tbl' WITH DELIMITER '|' NULL '\N'  CSV;
-- COPY SSB."DATE" FROM '/home/zeyuanhu/projects/eyalroz-ssb-dbgen/postgres/s1/date.tbl' WITH DELIMITER '|' NULL '\N'  CSV;

COPY SSB."CUSTOMER" FROM '/home/zeyuanhu/projects/eyalroz-ssb-dbgen/medium-sample-postgres/customer.tbl' WITH DELIMITER '|' NULL '\N'  CSV;
COPY SSB."LINEORDER" FROM '/home/zeyuanhu/projects/eyalroz-ssb-dbgen/medium-sample-postgres/lineorder.tbl' WITH DELIMITER '|' NULL '\N'  CSV;
COPY SSB."PART" FROM '/home/zeyuanhu/projects/eyalroz-ssb-dbgen/medium-sample-postgres/part.tbl' WITH DELIMITER '|' NULL '\N'  CSV;
COPY SSB."SUPPLIER" FROM '/home/zeyuanhu/projects/eyalroz-ssb-dbgen/medium-sample-postgres/supplier.tbl' WITH DELIMITER '|' NULL '\N'  CSV;
COPY SSB."DATE" FROM '/home/zeyuanhu/projects/eyalroz-ssb-dbgen/medium-sample-postgres/date.tbl' WITH DELIMITER '|' NULL '\N'  CSV;

-- COPY SSB."CUSTOMER" FROM '/home/zeyuanhu/projects/eyalroz-ssb-dbgen/small-sample-postgres/customer.tbl' WITH DELIMITER '|' NULL '\N'  CSV;
-- COPY SSB."LINEORDER" FROM '/home/zeyuanhu/projects/eyalroz-ssb-dbgen/small-sample-postgres/lineorder.tbl' WITH DELIMITER '|' NULL '\N'  CSV;
-- COPY SSB."PART" FROM '/home/zeyuanhu/projects/eyalroz-ssb-dbgen/small-sample-postgres/part.tbl' WITH DELIMITER '|' NULL '\N'  CSV;
-- COPY SSB."SUPPLIER" FROM '/home/zeyuanhu/projects/eyalroz-ssb-dbgen/small-sample-postgres/supplier.tbl' WITH DELIMITER '|' NULL '\N'  CSV;
-- COPY SSB."DATE" FROM '/home/zeyuanhu/projects/eyalroz-ssb-dbgen/small-sample-postgres/date.tbl' WITH DELIMITER '|' NULL '\N'  CSV;

COMMIT;

ALTER TABLE SSB."CUSTOMER" SET LOGGED;
ALTER TABLE SSB."LINEORDER" SET LOGGED;
ALTER TABLE SSB."PART" SET LOGGED;
ALTER TABLE SSB."SUPPLIER" SET LOGGED;
ALTER TABLE SSB."DATE" SET LOGGED;



