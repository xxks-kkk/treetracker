BEGIN TRANSACTION;

ALTER TABLE ssb_int.part
    ADD PRIMARY KEY (partkey);
ALTER TABLE ssb_int.supplier
    ADD PRIMARY KEY (suppkey);
ALTER TABLE ssb_int.date
    ADD PRIMARY KEY (datekey);
ALTER TABLE ssb_int.customer
    ADD PRIMARY KEY (custkey);

ALTER TABLE ssb_int.lineorder
    ADD CONSTRAINT fk_lineorder_part FOREIGN KEY (partkey) REFERENCES ssb_int.part (partkey),
    ADD CONSTRAINT fk_lineorder_supplier FOREIGN KEY (suppkey) REFERENCES ssb_int.supplier (suppkey),
    ADD CONSTRAINT fk_lineorder_date FOREIGN KEY (datekey) REFERENCES ssb_int.date(datekey),
    ADD CONSTRAINT fk_lineorder_customer FOREIGN KEY (custkey) REFERENCES ssb_int.customer(custkey);

COMMIT;

analyze ssb_int.part;
analyze ssb_int.supplier;
analyze ssb_int.customer;
analyze ssb_int.lineorder;
analyze ssb_int.date;