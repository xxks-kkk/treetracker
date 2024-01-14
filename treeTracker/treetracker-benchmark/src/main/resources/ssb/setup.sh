PROJECT_ROOT=/home/zeyuanhu/projects/treetracker2

psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/ssb/1p1.sql &&
psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/ssb/1p2.sql &&
psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/ssb/1p3.sql &&
psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/ssb/2p1.sql &&
psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/ssb/2p2.sql &&
psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/ssb/2p3.sql &&
psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/ssb/3p1.sql &&
psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/ssb/3p2.sql &&
psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/ssb/3p3.sql &&
psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/ssb/3p4.sql &&
psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/ssb/4p1.sql &&
psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/ssb/4p2.sql &&
psql -p5432 -d postgres -f ${PROJECT_ROOT}/treeTracker/treetracker-benchmark/src/main/resources/ssb/4p3.sql