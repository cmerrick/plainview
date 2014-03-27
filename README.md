# repwrite

tap into your database's replication system and write all change data down to a file

## usage

_currently only support MySQL row-based replication_

```bash
lein run -m repwrite.producer -f mysql-bin.000001 -n 8779 -P 5001 -u replication-user -p password -s kinesis-stream-name
lein run -m repwrite.consumer -a app-name -b bucket-name -s kinesis-stream-name
```

in your my.cnf:

```
server-id = 1234
log-bin=mysql-bin
binlog_format=row
```
