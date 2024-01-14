PROJECT_ROOT=/home/zeyuanhu/projects/treetracker2

psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/tpch/3.sql &&
psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/tpch/7a.sql &&
psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/tpch/7b.sql &&
psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/tpch/8.sql &&
psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/tpch/9.sql &&
psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/tpch/10.sql &&
psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/tpch/11.sql &&
psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/tpch/12.sql &&
psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/tpch/14.sql &&
psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/tpch/15.sql &&
psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/tpch/16.sql &&
psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/tpch/18.sql &&
psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/tpch/19a.sql &&
psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/tpch/19b.sql &&
psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/tpch/19c.sql &&
psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/tpch/20.sql