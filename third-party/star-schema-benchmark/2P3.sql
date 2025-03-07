SELECT count(*)
FROM
    lineorder,
    date,
    part,
    supplier
WHERE
    lo_orderdate = d_datekey
    AND lo_partkey = p_partkey
    AND lo_suppkey = s_suppkey
    AND p_brand1 = 'MFGR#2339'
    AND s_region = 'EUROPE';
