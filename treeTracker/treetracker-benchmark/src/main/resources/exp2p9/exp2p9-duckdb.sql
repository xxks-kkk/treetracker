-- Ingest data for Exp2.5: Impact of No-good Tuples at R_k
-- instruction:
-- duckdb /home/zeyuanhu/projects/treetracker2/exp29.duckdb < /home/zeyuanhu/projects/treetracker2-remote-dev/treeTracker/treetracker-benchmark/src/main/resources/exp2p9/exp2p9-duckdb.sql
BEGIN TRANSACTION;

DROP SCHEMA IF EXISTS exp2p9 CASCADE;
CREATE SCHEMA exp2p9;

CREATE TABLE exp2p9.U0 (c integer, e integer);
CREATE TABLE exp2p9.U1 (c integer, e integer);
CREATE TABLE exp2p9.U2 (c integer, e integer);
CREATE TABLE exp2p9.U3 (c integer, e integer);
CREATE TABLE exp2p9.U4 (c integer, e integer);
CREATE TABLE exp2p9.U5 (c integer, e integer);
CREATE TABLE exp2p9.U6 (c integer, e integer);
CREATE TABLE exp2p9.U7 (c integer, e integer);
CREATE TABLE exp2p9.U8 (c integer, e integer);
CREATE TABLE exp2p9.U9 (c integer, e integer);
CREATE TABLE exp2p9.U10 (c integer, e integer);
CREATE TABLE exp2p9.U20 (c integer, e integer);
CREATE TABLE exp2p9.U30 (c integer, e integer);
CREATE TABLE exp2p9.U40 (c integer, e integer);
CREATE TABLE exp2p9.U50 (c integer, e integer);
CREATE TABLE exp2p9.U60 (c integer, e integer);
CREATE TABLE exp2p9.U70 (c integer, e integer);
CREATE TABLE exp2p9.U80 (c integer, e integer);
CREATE TABLE exp2p9.U90 (c integer, e integer);
CREATE TABLE exp2p9.U100 (c integer, e integer);
CREATE TABLE exp2p9.R (a integer, c integer);
CREATE TABLE exp2p9.V (c integer, d integer);
CREATE TABLE exp2p9.W (d integer, f integer);


COPY exp2p9.U0 FROM '/home/zeyuanhu/projects/treetracker2-remote-dev/results/others/exp2p9/exp2.9U_0.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p9.U1 FROM '/home/zeyuanhu/projects/treetracker2-remote-dev/results/others/exp2p9/exp2.9U_1.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p9.U2 FROM '/home/zeyuanhu/projects/treetracker2-remote-dev/results/others/exp2p9/exp2.9U_2.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p9.U3 FROM '/home/zeyuanhu/projects/treetracker2-remote-dev/results/others/exp2p9/exp2.9U_3.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p9.U4 FROM '/home/zeyuanhu/projects/treetracker2-remote-dev/results/others/exp2p9/exp2.9U_4.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p9.U5 FROM '/home/zeyuanhu/projects/treetracker2-remote-dev/results/others/exp2p9/exp2.9U_5.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p9.U6 FROM '/home/zeyuanhu/projects/treetracker2-remote-dev/results/others/exp2p9/exp2.9U_6.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p9.U7 FROM '/home/zeyuanhu/projects/treetracker2-remote-dev/results/others/exp2p9/exp2.9U_7.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p9.U8 FROM '/home/zeyuanhu/projects/treetracker2-remote-dev/results/others/exp2p9/exp2.9U_8.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p9.U9 FROM '/home/zeyuanhu/projects/treetracker2-remote-dev/results/others/exp2p9/exp2.9U_9.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p9.U10 FROM '/home/zeyuanhu/projects/treetracker2-remote-dev/results/others/exp2p9/exp2.9U_10.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p9.U20 FROM '/home/zeyuanhu/projects/treetracker2-remote-dev/results/others/exp2p9/exp2.9U_20.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p9.U30 FROM '/home/zeyuanhu/projects/treetracker2-remote-dev/results/others/exp2p9/exp2.9U_30.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p9.U40 FROM '/home/zeyuanhu/projects/treetracker2-remote-dev/results/others/exp2p9/exp2.9U_40.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p9.U50 FROM '/home/zeyuanhu/projects/treetracker2-remote-dev/results/others/exp2p9/exp2.9U_50.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p9.U60 FROM '/home/zeyuanhu/projects/treetracker2-remote-dev/results/others/exp2p9/exp2.9U_60.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p9.U70 FROM '/home/zeyuanhu/projects/treetracker2-remote-dev/results/others/exp2p9/exp2.9U_70.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p9.U80 FROM '/home/zeyuanhu/projects/treetracker2-remote-dev/results/others/exp2p9/exp2.9U_80.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p9.U90 FROM '/home/zeyuanhu/projects/treetracker2-remote-dev/results/others/exp2p9/exp2.9U_90.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p9.U100 FROM '/home/zeyuanhu/projects/treetracker2-remote-dev/results/others/exp2p9/exp2.9U_100.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p9.R FROM '/home/zeyuanhu/projects/treetracker2-remote-dev/results/others/exp2p9/exp2.9R.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p9.V FROM '/home/zeyuanhu/projects/treetracker2-remote-dev/results/others/exp2p9/exp2.9V.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p9.W FROM '/home/zeyuanhu/projects/treetracker2-remote-dev/results/others/exp2p9/exp2.9W.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;


COMMIT;