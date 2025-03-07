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
    AND c_region = 'ASIA'
    AND s_region = 'ASIA'
    AND d_year >= 1992
    AND d_year <= 1997;
