#!/usr/bin/sh

service postgresql stop &&
sync &&
sh -c "echo 3 > /proc/sys/vm/drop_caches" &&
service postgresql start