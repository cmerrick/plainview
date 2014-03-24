# repwrite

tap into your database's replication system and write all change data down to a file

## usage

_currently only support MySQL row-based replication_

```bash
lein run -m repwrite.core -f mysql-bin.000001 -n 8779 -p 5001
```

in your my.cnf:

```
server-id = 1234
log-bin=mysql-bin
binlog_format=row
```
