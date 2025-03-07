/*+ Leading( (((lineorder date) customer) supplier) )
*/
SELECT
    count(*)
FROM
    customer,
    lineorder,
    supplier,
    date
WHERE
    lo_custkey = c_custkey
    AND lo_suppkey = s_suppkey
    AND lo_orderdate = d_datekey
    AND (c_city='UNITED KI1' or c_city='UNITED KI5')
    AND (s_city='UNITED KI1' or s_city='UNITED KI5')
    AND d_year >= 1992
    AND d_year <= 1997;
