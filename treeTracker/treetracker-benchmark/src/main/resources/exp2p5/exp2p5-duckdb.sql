-- Ingest data for Exp2.5: Impact of No-good Tuples at R_k
-- instruction:
-- duckdb /home/zeyuanhu/projects/treetracker2/exp25.duckdb < /home/zeyuanhu/projects/treetracker2/treeTracker/treetracker-benchmark/src/main/resources/exp2p5/exp2p5-duckdb.sql
BEGIN TRANSACTION;

DROP SCHEMA IF EXISTS exp2p5 CASCADE;
CREATE SCHEMA exp2p5;

CREATE TABLE exp2p5.T0 (a integer, b integer);
CREATE TABLE exp2p5.T10 (a integer, b integer);
CREATE TABLE exp2p5.T20 (a integer, b integer);
CREATE TABLE exp2p5.T30 (a integer, b integer);
CREATE TABLE exp2p5.T40 (a integer, b integer);
CREATE TABLE exp2p5.T50 (a integer, b integer);
CREATE TABLE exp2p5.T60 (a integer, b integer);
CREATE TABLE exp2p5.T70 (a integer, b integer);
CREATE TABLE exp2p5.T80 (a integer, b integer);
CREATE TABLE exp2p5.T90 (a integer, b integer);
CREATE TABLE exp2p5.T100 (a integer, b integer);
CREATE TABLE exp2p5.R (a integer);
CREATE TABLE exp2p5.S (b integer);


COPY exp2p5.T0 FROM '/home/zeyuanhu/projects/treetracker2/results/others/exp2p5/exp2.5T_0.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p5.T10 FROM '/home/zeyuanhu/projects/treetracker2/results/others/exp2p5/exp2.5T_10.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p5.T20 FROM '/home/zeyuanhu/projects/treetracker2/results/others/exp2p5/exp2.5T_20.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p5.T30 FROM '/home/zeyuanhu/projects/treetracker2/results/others/exp2p5/exp2.5T_30.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p5.T40 FROM '/home/zeyuanhu/projects/treetracker2/results/others/exp2p5/exp2.5T_40.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p5.T50 FROM '/home/zeyuanhu/projects/treetracker2/results/others/exp2p5/exp2.5T_50.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p5.T60 FROM '/home/zeyuanhu/projects/treetracker2/results/others/exp2p5/exp2.5T_60.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p5.T70 FROM '/home/zeyuanhu/projects/treetracker2/results/others/exp2p5/exp2.5T_70.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p5.T80 FROM '/home/zeyuanhu/projects/treetracker2/results/others/exp2p5/exp2.5T_80.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p5.T90 FROM '/home/zeyuanhu/projects/treetracker2/results/others/exp2p5/exp2.5T_90.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p5.T100 FROM '/home/zeyuanhu/projects/treetracker2/results/others/exp2p5/exp2.5T_100.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p5.R FROM '/home/zeyuanhu/projects/treetracker2/results/others/exp2p5/exp2.5R.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p5.S FROM '/home/zeyuanhu/projects/treetracker2/results/others/exp2p5/exp2.5S.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;

COMMIT;