# We only need to use SQLite to obtain its join order on SSB queries
# We don't need to actually set it up with the goal of running queries using our own engine.

PROJECT_DIR=/home/zeyuanhu/projects/treetracker2/

(cd $PROJECT_DIR/treeTracker/treetracker-benchmark/src/main/resources/sqlite/ssb/ &&
sqlite3 $PROJECT_DIR/ssb.sqlitedb < $PROJECT_DIR/treeTracker/treetracker-benchmark/src/main/resources/sqlite/ssb/setup2)