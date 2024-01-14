analyze tpch_int.part;
analyze tpch_int.supplier;
analyze tpch_int.partsupp;
analyze tpch_int.customer;
analyze tpch_int.orders;
analyze tpch_int.lineitem;
analyze tpch_int.nation;
analyze tpch_int.region;

BEGIN TRANSACTION;

ALTER TABLE tpch_int.part
    ADD PRIMARY KEY (partkey);
ALTER TABLE tpch_int.supplier
    ADD PRIMARY KEY (suppkey);
ALTER TABLE tpch_int.customer
    ADD PRIMARY KEY (custkey);
ALTER TABLE tpch_int.orders
    ADD PRIMARY KEY (orderkey);
ALTER TABLE tpch_int.nation
    ADD PRIMARY KEY (nationkey);
ALTER TABLE tpch_int.region
    ADD PRIMARY KEY (regionkey);

ALTER TABLE tpch_int.supplier
    ADD CONSTRAINT fk_supplier_nation FOREIGN KEY (nationkey) REFERENCES tpch_int.nation(nationkey);
ALTER TABLE tpch_int.partsupp
    ADD CONSTRAINT fk_partsupp_part FOREIGN KEY (partkey) REFERENCES tpch_int.part(partkey),
    ADD CONSTRAINT fk_partsupp_supplier FOREIGN KEY (suppkey) REFERENCES tpch_int.supplier(suppkey),
    ADD UNIQUE (partkey, suppkey);
ALTER TABLE tpch_int.customer
    ADD CONSTRAINT fk_customer_nation FOREIGN KEY (nationkey) REFERENCES tpch_int.nation(nationkey);
ALTER TABLE tpch_int.orders
    ADD CONSTRAINT fk_orders_customer FOREIGN KEY (custkey) REFERENCES tpch_int.customer(custkey);
ALTER TABLE tpch_int.lineitem
    ADD CONSTRAINT fk_lineitem_orders FOREIGN KEY (orderkey) REFERENCES tpch_int.orders(orderkey),
    ADD CONSTRAINT fk_lineitem_partsupp FOREIGN KEY (partkey, suppkey) REFERENCES tpch_int.partsupp(partkey, suppkey);
ALTER TABLE tpch_int.nation
    ADD CONSTRAINT fk_nation_region FOREIGN KEY (regionkey) REFERENCES tpch_int.region(regionkey);

COMMIT;