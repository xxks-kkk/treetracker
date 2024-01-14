BEGIN TRANSACTION;

DROP VIEW IF EXISTS tpch.q7aw_nation1 CASCADE;
DROP VIEW IF EXISTS tpch.q7aw_nation2 CASCADE;
DROP VIEW IF EXISTS tpch.q7aw_customer CASCADE;
DROP VIEW IF EXISTS tpch.q7aw_lineitem CASCADE;

CREATE VIEW tpch.q7aw_nation1 AS
SELECT nationkey
FROM tpch.nation
WHERE N_NAME = 'FRANCE';

CREATE VIEW tpch.q7aw_nation2 AS
SELECT nationkey as nationkey2
FROM tpch.nation
WHERE N_NAME = 'GERMANY';

CREATE VIEW tpch.q7aw_customer AS
SELECT custkey, nationkey as nationkey2
FROM tpch.customer;

CREATE VIEW tpch.q7aw_lineitem AS
SELECT orderkey, partkey, suppkey
FROM tpch.lineitem
WHERE l_shipdate between date '1995-01-01' and date '1996-12-31';

END;