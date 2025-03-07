/*+ Leading( ((((lineorder customer) supplier) date) part) )
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
    AND c_nation = 'UNITED STATES'
    AND s_nation = 'UNITED STATES'
    AND p_category = 'MFGR#14'
    AND (d_year = 1997 or d_year = 1998);
