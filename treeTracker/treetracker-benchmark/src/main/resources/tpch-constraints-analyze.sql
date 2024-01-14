analyze TPCH.part;
analyze TPCH.supplier;
analyze TPCH.partsupp;
analyze TPCH.customer;
analyze TPCH.orders;
analyze TPCH.lineitem;
analyze TPCH.nation;
analyze TPCH.region;

BEGIN TRANSACTION;

ALTER TABLE tpch.part
    ADD PRIMARY KEY (partkey);
ALTER TABLE tpch.supplier
    ADD PRIMARY KEY (suppkey);
ALTER TABLE tpch.customer
    ADD PRIMARY KEY (custkey);
ALTER TABLE tpch.orders
    ADD PRIMARY KEY (orderkey);
ALTER TABLE tpch.nation
    ADD PRIMARY KEY (nationkey);
ALTER TABLE tpch.region
    ADD PRIMARY KEY (regionkey);

ALTER TABLE tpch.supplier
    ADD CONSTRAINT fk_supplier_nation FOREIGN KEY (nationkey) REFERENCES tpch.nation(nationkey);
ALTER TABLE tpch.partsupp
    ADD CONSTRAINT fk_partsupp_part FOREIGN KEY (partkey) REFERENCES tpch.part(partkey),
    ADD CONSTRAINT fk_partsupp_supplier FOREIGN KEY (suppkey) REFERENCES tpch.supplier(suppkey),
    ADD UNIQUE (partkey, suppkey);
ALTER TABLE tpch.customer
    ADD CONSTRAINT fk_customer_nation FOREIGN KEY (nationkey) REFERENCES tpch.nation(nationkey);
ALTER TABLE tpch.orders
    ADD CONSTRAINT fk_orders_customer FOREIGN KEY (custkey) REFERENCES tpch.customer(custkey);
ALTER TABLE tpch.lineitem
    ADD CONSTRAINT fk_lineitem_orders FOREIGN KEY (orderkey) REFERENCES tpch.orders(orderkey),
    ADD CONSTRAINT fk_lineitem_partsupp FOREIGN KEY (partkey, suppkey) REFERENCES tpch.partsupp(partkey, suppkey);
ALTER TABLE tpch.nation
    ADD CONSTRAINT fk_nation_region FOREIGN KEY (regionkey) REFERENCES tpch.region(regionkey);

COMMIT;