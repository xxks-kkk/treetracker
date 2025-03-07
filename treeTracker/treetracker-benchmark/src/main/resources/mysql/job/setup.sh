# Note I never run those commands in automatically fashion; I accumulate those steps by executing SQLs manually in mysql
# shell. The steps are working; it's just I never run them automatically to verify the automation itself works properly.
mysql -u admin -p -e "CREATE DATABASE IF NOT EXISTS imdb";
mysql --local-infile=1 -u admin -p -D imdb < imdb-original-mysql.sql
mysql --local-infile=1 -u admin -p -D imdb < ingest-data.sql
mysql -u admin -p -D imdb < imdb-mysql-constraints-analyze.sql