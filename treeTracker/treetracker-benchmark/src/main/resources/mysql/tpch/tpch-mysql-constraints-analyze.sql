START TRANSACTION;

ALTER TABLE tpch.supplier
    ADD CONSTRAINT fk_supplier_nation FOREIGN KEY (s_nationkey) REFERENCES tpch.nation(n_nationkey);
ALTER TABLE tpch.partsupp
    ADD CONSTRAINT fk_partsupp_part FOREIGN KEY (ps_partkey) REFERENCES tpch.part(p_partkey),
    ADD CONSTRAINT fk_partsupp_supplier FOREIGN KEY (ps_suppkey) REFERENCES tpch.supplier(s_suppkey),
    ADD UNIQUE (ps_partkey, ps_suppkey);
ALTER TABLE tpch.customer
    ADD CONSTRAINT fk_customer_nation FOREIGN KEY (c_nationkey) REFERENCES tpch.nation(n_nationkey);
ALTER TABLE tpch.orders
    ADD CONSTRAINT fk_orders_customer FOREIGN KEY (o_custkey) REFERENCES tpch.customer(c_custkey);
ALTER TABLE tpch.lineitem
    ADD CONSTRAINT fk_lineitem_orders FOREIGN KEY (l_orderkey) REFERENCES tpch.orders(o_orderkey),
    ADD CONSTRAINT fk_lineitem_partsupp FOREIGN KEY (l_partkey, l_suppkey) REFERENCES tpch.partsupp(ps_partkey, ps_suppkey);
ALTER TABLE tpch.nation
    ADD CONSTRAINT fk_nation_region FOREIGN KEY (n_regionkey) REFERENCES tpch.region(r_regionkey);


COMMIT;

ANALYZE TABLE tpch.nation;
ANALYZE TABLE tpch.region;
ANALYZE TABLE tpch.part;
ANALYZE TABLE tpch.supplier;
ANALYZE TABLE tpch.partsupp;
ANALYZE TABLE tpch.customer;
ANALYZE TABLE tpch.orders;
ANALYZE TABLE tpch.lineitem;