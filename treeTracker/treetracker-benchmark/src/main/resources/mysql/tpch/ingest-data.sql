SET GLOBAL local_infile=1;
use tpch;

LOAD DATA LOCAL INFILE '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s1/part.tbl'
    INTO TABLE tpch.part
    FIELDS TERMINATED BY "|"
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY "\n";

LOAD DATA LOCAL INFILE '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s1/supplier.tbl'
    INTO TABLE tpch.supplier
    FIELDS TERMINATED BY "|"
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY "\n";

LOAD DATA LOCAL INFILE '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s1/partsupp.tbl'
    INTO TABLE tpch.partsupp
    FIELDS TERMINATED BY "|"
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY "\n";

LOAD DATA LOCAL INFILE '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s1/customer.tbl'
    INTO TABLE tpch.customer
    FIELDS TERMINATED BY "|"
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY "\n";

LOAD DATA LOCAL INFILE '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s1/orders.tbl'
    INTO TABLE tpch.orders
         FIELDS TERMINATED BY "|"
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY "\n";

LOAD DATA LOCAL INFILE '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s1/lineitem.tbl'
    INTO TABLE tpch.lineitem
         FIELDS TERMINATED BY "|"
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY "\n";

LOAD DATA LOCAL INFILE '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s1/nation.tbl'
    INTO TABLE tpch.nation
    FIELDS TERMINATED BY "|"
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY "\n";

LOAD DATA LOCAL INFILE '/home/zeyuanhu/projects/tpch-kit/dbgen/postgres/s1/region.tbl'
    INTO TABLE tpch.region
    FIELDS TERMINATED BY "|"
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY "\n";