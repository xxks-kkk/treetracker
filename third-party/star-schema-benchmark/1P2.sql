SELECT count(*)
FROM lineorder, date
WHERE
     lo_orderdate = d_datekey
     AND D_YEARMONTHNUM = 199401
     AND lo_discount BETWEEN 4 AND 6
     AND lo_quantity BETWEEN 26 AND 35;
