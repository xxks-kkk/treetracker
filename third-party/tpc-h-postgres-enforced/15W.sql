/*+ Leading( (lineitemrevenue supplier) )
  */
SELECT
    count(*)
from
    supplier,
    lineitemrevenue -- assume existence of lineitemrevenue view
where
    s_suppkey = supplier_no;
