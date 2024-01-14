BEGIN TRANSACTION;

ALTER TABLE ssb.part
    ADD PRIMARY KEY (partkey);
ALTER TABLE ssb.supplier
    ADD PRIMARY KEY (suppkey);
ALTER TABLE ssb.date
    ADD PRIMARY KEY (datekey);
ALTER TABLE ssb.customer
    ADD PRIMARY KEY (custkey);

ALTER TABLE ssb.lineorder
    ADD CONSTRAINT fk_lineorder_part FOREIGN KEY (partkey) REFERENCES ssb.part (partkey),
    ADD CONSTRAINT fk_lineorder_supplier FOREIGN KEY (suppkey) REFERENCES ssb.supplier (suppkey),
    ADD CONSTRAINT fk_lineorder_date FOREIGN KEY (datekey) REFERENCES ssb.date(datekey),
    ADD CONSTRAINT fk_lineorder_customer FOREIGN KEY (custkey) REFERENCES ssb.customer(custkey);

COMMIT;

analyze ssb.part;
analyze ssb.supplier;
analyze ssb.customer;
analyze ssb.lineorder;
analyze ssb.date;