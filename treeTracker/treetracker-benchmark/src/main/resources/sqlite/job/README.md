# Steps to obtain SQLite join orders for JOB queries

1. we run `imdb-orginal.sql` to set up the imdb database using [original JOB schema](https://github.com/gregrahn/join-order-benchmark/blob/master/schema.sql)
2. We export database from Postgres to csvs by running the sqls in the second block of `ingest-data-creation.txt`
3. Run `setup.sh` with `setup2` to ingest data

## Catches on SQLite

- NULL is treated as empty string "". Thus, for predicate
  `cn.country_code !='[pl]'`, even `cn.country_code` is NULL for a tuple, SQLite
  will select the tuple because `"" != '[pl]'` is true. However, in Postgres,
  the tuple will not be selected because `NULL != '[pl]'` is not true.
    - Similarly, `t.production_year > 2005` also needs to be supplemented with
      `t.production_year != ""`.
- Make sure `PRAGMA case_sensitive_like=ON;` because otherwise, if a tuple `name` field has
  `'1 2FILM'`, it will be selected under `cn.name LIKE '%Film%'`.