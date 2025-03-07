# Because TPC-H plan has some complexities for automated processing and there are not many relations involved for
# each query, this function simply print out all the plans, which to be manually edited into our json files.
# We assume each raw file has "explain query plan" attached due to some query involves defining views.
# We use bash to obtain orders because JDBC driver doesn't support certain functions, e.g., date_part
# Since those function may appear in the SELECT fields and we may want to comment them out as a workaround. However,
# such workaround impacts join order.

PROJECT_DIR=/home/zeyuanhu/projects/treetracker2
TPC_H_QUERIES_DIR=$PROJECT_DIR/third-party/tpc-h

echo "processing 10W.sql"
(cd $PROJECT_DIR/treeTracker/treetracker-benchmark/src/main/resources/sqlite/tpch/ &&
sqlite3 $PROJECT_DIR/tpch.sqlitedb ".read $TPC_H_QUERIES_DIR/10W.sql")

echo "processing 11W.sql"
(cd $PROJECT_DIR/treeTracker/treetracker-benchmark/src/main/resources/sqlite/tpch/ &&
sqlite3 $PROJECT_DIR/tpch.sqlitedb ".read $TPC_H_QUERIES_DIR/11W.sql")

echo "processing 12W.sql"
(cd $PROJECT_DIR/treeTracker/treetracker-benchmark/src/main/resources/sqlite/tpch/ &&
sqlite3 $PROJECT_DIR/tpch.sqlitedb ".read $TPC_H_QUERIES_DIR/12W.sql")

echo "processing 14W.sql"
(cd $PROJECT_DIR/treeTracker/treetracker-benchmark/src/main/resources/sqlite/tpch/ &&
sqlite3 $PROJECT_DIR/tpch.sqlitedb ".read $TPC_H_QUERIES_DIR/14W.sql")

echo "processing 15W.sql"
(cd $PROJECT_DIR/treeTracker/treetracker-benchmark/src/main/resources/sqlite/tpch/ &&
sqlite3 $PROJECT_DIR/tpch.sqlitedb ".read $TPC_H_QUERIES_DIR/15W.sql")

echo "processing 16W.sql"
(cd $PROJECT_DIR/treeTracker/treetracker-benchmark/src/main/resources/sqlite/tpch/ &&
sqlite3 $PROJECT_DIR/tpch.sqlitedb ".read $TPC_H_QUERIES_DIR/16W.sql")

echo "processing 18W.sql"
(cd $PROJECT_DIR/treeTracker/treetracker-benchmark/src/main/resources/sqlite/tpch/ &&
sqlite3 $PROJECT_DIR/tpch.sqlitedb ".read $TPC_H_QUERIES_DIR/18W.sql")

echo "processing 19aW.sql"
(cd $PROJECT_DIR/treeTracker/treetracker-benchmark/src/main/resources/sqlite/tpch/ &&
sqlite3 $PROJECT_DIR/tpch.sqlitedb ".read $TPC_H_QUERIES_DIR/19aW.sql")

echo "processing 19bW.sql"
(cd $PROJECT_DIR/treeTracker/treetracker-benchmark/src/main/resources/sqlite/tpch/ &&
sqlite3 $PROJECT_DIR/tpch.sqlitedb ".read $TPC_H_QUERIES_DIR/19bW.sql")

echo "processing 19cW.sql"
(cd $PROJECT_DIR/treeTracker/treetracker-benchmark/src/main/resources/sqlite/tpch/ &&
sqlite3 $PROJECT_DIR/tpch.sqlitedb ".read $TPC_H_QUERIES_DIR/19cW.sql")

echo "processing 20W.sql"
(cd $PROJECT_DIR/treeTracker/treetracker-benchmark/src/main/resources/sqlite/tpch/ &&
sqlite3 $PROJECT_DIR/tpch.sqlitedb ".read $TPC_H_QUERIES_DIR/20W.sql")

echo "processing 3W.sql"
(cd $PROJECT_DIR/treeTracker/treetracker-benchmark/src/main/resources/sqlite/tpch/ &&
sqlite3 $PROJECT_DIR/tpch.sqlitedb ".read $TPC_H_QUERIES_DIR/3W.sql")

echo "processing 7aW.sql"
(cd $PROJECT_DIR/treeTracker/treetracker-benchmark/src/main/resources/sqlite/tpch/ &&
sqlite3 $PROJECT_DIR/tpch.sqlitedb ".read $TPC_H_QUERIES_DIR/7aW.sql")

echo "processing 7bW.sql"
(cd $PROJECT_DIR/treeTracker/treetracker-benchmark/src/main/resources/sqlite/tpch/ &&
sqlite3 $PROJECT_DIR/tpch.sqlitedb ".read $TPC_H_QUERIES_DIR/7bW.sql")

echo "processing 8W.sql"
(cd $PROJECT_DIR/treeTracker/treetracker-benchmark/src/main/resources/sqlite/tpch/ &&
sqlite3 $PROJECT_DIR/tpch.sqlitedb ".read $TPC_H_QUERIES_DIR/8W.sql")

echo "processing 9W.sql"
(cd $PROJECT_DIR/treeTracker/treetracker-benchmark/src/main/resources/sqlite/tpch/ &&
sqlite3 $PROJECT_DIR/tpch.sqlitedb ".read $TPC_H_QUERIES_DIR/9W.sql")