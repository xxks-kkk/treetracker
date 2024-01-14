-- Q1.1
--- 118735
SELECT count(*)
FROM ssb.lineorder, ssb.date
WHERE lineorder.datekey = date.datekey
AND d_year = 1993
AND lo_discount BETWEEN 1 AND 3
AND lo_quantity < 25;

SELECT count(*)
FROM ssb.q1p1_lineorder natural join
    ssb.q1p1_date;

-- Q1.2
--- 4251
SELECT count(*)
FROM ssb.lineorder, ssb.date
WHERE lineorder.datekey = date.datekey
  AND D_YEARMONTHNUM = 199401
  AND lo_discount BETWEEN 4 AND 6
  AND lo_quantity BETWEEN 26 AND 35;

SELECT count(*)
FROM ssb.q1p2_lineorder natural join
    ssb.q1p2_date;

-- Q1.3
--- 470
SELECT count(*)
FROM ssb.lineorder, ssb.date
WHERE lineorder.datekey = date.datekey
  AND d_weeknuminyear = 6
  AND d_year = 1994
  AND lo_discount BETWEEN 5 AND 7
  AND lo_quantity BETWEEN 36 AND 40;

SELECT count(*)
FROM ssb.q1p3_date natural join
    ssb.q1p3_lineorder;

-- Q2.1
--- 46026
SELECT count(*)
FROM ssb.lineorder, ssb.date, ssb.part, ssb.supplier
WHERE lineorder.datekey = date.datekey
AND lineorder.partkey = part.partkey
AND lineorder.suppkey = supplier.suppkey
AND p_category = 'MFGR#12'
AND s_region = 'AMERICA';

SELECT count(*)
FROM ssb_int.lineorder natural join
    ssb.q2p1_part natural join
    ssb.q2p1_supplier natural join
    ssb_int.date;

-- Q2.2
--- 10577
SELECT count(*)
FROM ssb.lineorder, ssb.date, ssb.part, ssb.supplier
WHERE lineorder.datekey = date.datekey
  AND lineorder.partkey = part.partkey
  AND lineorder.suppkey = supplier.suppkey
  AND p_brand1 BETWEEN 'MFGR#2221' AND 'MFGR#2228'
  AND s_region = 'ASIA';

SELECT count(*)
FROM ssb_int.lineorder natural join
    ssb_int.date natural join
    ssb.q2p2_part natural join
    ssb.q2p2_supplier;

-- Q2.3
--- 1222
SELECT count(*)
FROM ssb.lineorder, ssb.date, ssb.part, ssb.supplier
WHERE lineorder.datekey = date.datekey
  AND lineorder.partkey = part.partkey
  AND lineorder.suppkey = supplier.suppkey
  AND p_brand1 = 'MFGR#2339'
  AND s_region = 'EUROPE';

SELECT count(*)
FROM ssb_int.lineorder natural join
     ssb_int.date natural join
     ssb.q2p3_part natural join
     ssb.q2p3_supplier;

-- Q3.1
--- 246821
SELECT count(*)
FROM ssb.customer, ssb.lineorder, ssb.supplier, ssb.date
WHERE lineorder.custkey = customer.custkey
AND lineorder.suppkey = supplier.suppkey
AND lineorder.datekey = date.datekey
AND c_region = 'ASIA'
AND s_region = 'ASIA'
AND d_year >= 1992
AND d_year <= 1997;

SELECT count(*)
FROM ssb_int.lineorder natural join
    ssb.q3p1_customer natural join
    ssb.q3p1_supplier natural join
    ssb.q3p1_date;

-- Q3.2
--- 8606
SELECT count(*)
FROM ssb.customer, ssb.lineorder, ssb.supplier, ssb.date
WHERE lineorder.custkey = customer.custkey
  AND lineorder.suppkey = supplier.suppkey
  AND lineorder.datekey = date.datekey
  AND c_nation = 'UNITED STATES'
  AND s_nation = 'UNITED STATES'
  AND d_year >= 1992
  AND d_year <= 1997;

SELECT count(*)
FROM ssb.q3p2_customer natural join
    ssb.q3p2_date natural join
    ssb.q3p2_supplier natural join
    ssb_int.lineorder;

-- Q3.3
--- 339
SELECT count(*)
FROM ssb.customer, ssb.lineorder, ssb.supplier, ssb.date
WHERE lineorder.custkey = customer.custkey
  AND lineorder.suppkey = supplier.suppkey
  AND lineorder.datekey = date.datekey
  AND (c_city='UNITED KI1' or c_city='UNITED KI5')
  AND (s_city='UNITED KI1' or s_city='UNITED KI5')
  AND d_year >= 1992
  AND d_year <= 1997;

SELECT count(*)
FROM ssb.q3p3_customer natural join
     ssb.q3p3_date natural join
     ssb.q3p3_supplier natural join
     ssb_int.lineorder;

-- Q3.4
--- 5
SELECT count(*)
FROM ssb.customer, ssb.lineorder, ssb.supplier, ssb.date
WHERE lineorder.custkey = customer.custkey
  AND lineorder.suppkey = supplier.suppkey
  AND lineorder.datekey = date.datekey
  AND (c_city='UNITED KI1' or c_city='UNITED KI5')
  AND (s_city='UNITED KI1' or s_city='UNITED KI5')
  AND d_yearmonth = 'Dec1997';

SELECT count(*)
FROM ssb.q3p4_customer natural join
     ssb.q3p4_date natural join
     ssb.q3p4_supplier natural join
     ssb_int.lineorder;

-- Q4.1
--- 90353
SELECT count(*)
FROM ssb.customer, ssb.supplier, ssb.part, ssb.lineorder, ssb.date
WHERE lineorder.custkey = customer.custkey
AND lineorder.suppkey = supplier.suppkey
AND lineorder.partkey = part.partkey
AND lineorder.datekey = date.datekey
AND c_region = 'AMERICA'
AND s_region = 'AMERICA'
AND (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2');

SELECT count(*)
FROM ssb.q4p1_customer natural join
    ssb.q4p1_supplier natural join
    ssb.q4p1_part natural join
    ssb_int.lineorder natural join
    ssb_int.date;

-- Q4.2
--- 21803
SELECT count(*)
FROM ssb.customer, ssb.supplier, ssb.part, ssb.lineorder, ssb.date
WHERE lineorder.custkey = customer.custkey
  AND lineorder.suppkey = supplier.suppkey
  AND lineorder.partkey = part.partkey
  AND lineorder.datekey = date.datekey
  AND c_region = 'AMERICA'
  AND s_region = 'AMERICA'
  AND (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')
  AND (d_year = 1997 or d_year = 1998);

SELECT count(*)
FROM ssb.q4p2_customer natural join
    ssb.q4p2_supplier natural join
    ssb.q4p2_date natural join
    ssb.q4p2_part natural join
    ssb_int.lineorder;

-- Q4.3
--- 99
SELECT count(*)
FROM ssb.customer, ssb.supplier, ssb.part, ssb.lineorder, ssb.date
WHERE lineorder.custkey = customer.custkey
  AND lineorder.suppkey = supplier.suppkey
  AND lineorder.partkey = part.partkey
  AND lineorder.datekey = date.datekey
  AND c_nation = 'UNITED STATES'
  AND s_nation = 'UNITED STATES'
  AND p_category = 'MFGR#14'
  AND (d_year = 1997 or d_year = 1998);

SELECT count(*)
FROM ssb.q4p3_customer natural join
    ssb.q4p3_date natural join
    ssb.q4p3_part natural join
    ssb.q4p3_supplier natural join
    ssb_int.lineorder;