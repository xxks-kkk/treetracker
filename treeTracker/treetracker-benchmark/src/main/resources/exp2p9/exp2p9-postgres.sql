-- Ingest data for Exp2.5: Impact of No-good Tuples at R_k
-- instruction:
-- /Applications/Postgres.app/Contents/Versions/14/bin/psql -p5432 -d postgres -f  /Users/niyixuan/projects/treetracker2-local/treeTracker/treetracker-benchmark/src/main/resources/exp2p9/exp2p9-postgres.sql
BEGIN TRANSACTION;

DROP SCHEMA IF EXISTS exp2p9 CASCADE;
CREATE SCHEMA exp2p9;

CREATE TABLE exp2p9.U0
(
    c integer,
    e integer
);
CREATE TABLE exp2p9.U10
(
    c integer,
    e integer
);
CREATE TABLE exp2p9.U20
(
    c integer,
    e integer
);
CREATE TABLE exp2p9.U30
(
    c integer,
    e integer
);
CREATE TABLE exp2p9.U40
(
    c integer,
    e integer
);
CREATE TABLE exp2p9.U50
(
    c integer,
    e integer
);
CREATE TABLE exp2p9.U60
(
    c integer,
    e integer
);
CREATE TABLE exp2p9.U70
(
    c integer,
    e integer
);
CREATE TABLE exp2p9.U80
(
    c integer,
    e integer
);
CREATE TABLE exp2p9.U90
(
    c integer,
    e integer
);
CREATE TABLE exp2p9.U100
(
    c integer,
    e integer
);
CREATE TABLE exp2p9.R
(
    a integer,
    c integer
);
CREATE TABLE exp2p9.V
(
    c integer,
    d integer
);
CREATE TABLE exp2p9.W
(
    d integer,
    f integer
);


COPY exp2p9.R FROM '/Users/niyixuan/projects/treetracker2-local/results/others/exp2p9/exp2.9R.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p9.U0 FROM '/Users/niyixuan/projects/treetracker2-local/results/others/exp2p9/exp2.9U_0.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p9.U10 FROM '/Users/niyixuan/projects/treetracker2-local/results/others/exp2p9/exp2.9U_10.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p9.U20 FROM '/Users/niyixuan/projects/treetracker2-local/results/others/exp2p9/exp2.9U_20.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p9.U30 FROM '/Users/niyixuan/projects/treetracker2-local/results/others/exp2p9/exp2.9U_30.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p9.U40 FROM '/Users/niyixuan/projects/treetracker2-local/results/others/exp2p9/exp2.9U_40.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p9.U50 FROM '/Users/niyixuan/projects/treetracker2-local/results/others/exp2p9/exp2.9U_50.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p9.U60 FROM '/Users/niyixuan/projects/treetracker2-local/results/others/exp2p9/exp2.9U_60.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p9.U70 FROM '/Users/niyixuan/projects/treetracker2-local/results/others/exp2p9/exp2.9U_70.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p9.U80 FROM '/Users/niyixuan/projects/treetracker2-local/results/others/exp2p9/exp2.9U_80.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p9.U90 FROM '/Users/niyixuan/projects/treetracker2-local/results/others/exp2p9/exp2.9U_90.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p9.U100 FROM '/Users/niyixuan/projects/treetracker2-local/results/others/exp2p9/exp2.9U_100.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p9.V FROM '/Users/niyixuan/projects/treetracker2-local/results/others/exp2p9/exp2.9V.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;
COPY exp2p9.W FROM '/Users/niyixuan/projects/treetracker2-local/results/others/exp2p9/exp2.9W.csv' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '"' ESCAPE AS '\' CSV;


COMMIT;