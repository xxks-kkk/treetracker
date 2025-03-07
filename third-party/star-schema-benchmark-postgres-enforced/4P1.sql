/*+ Leading( ((((lineorder customer) supplier) part) date) )
*/
SELECT
    count(*)
FROM
    customer,
    supplier,
    part,
    lineorder,
    date
WHERE
    lo_custkey = c_custkey
    AND lo_suppkey = s_suppkey
    AND lo_partkey = p_partkey
    AND lo_orderdate = d_datekey
    AND c_region = 'AMERICA'
    AND s_region = 'AMERICA'
    AND (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2');
