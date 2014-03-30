# plainview

A toolkit for building a data pipeline from your database's binary log stream.

### background

The need to maintain secondary datasources like search indices, caches, and analytics data warehouses often leads to poorly designed infrastructures suffering from highly coupled application code and untimely batch data exportation. Use this toolkit to turn your primary data store's binary log into a realtime pipeline of raw data on which to build a robust data infrastructure.

Credit goes to the folks at LinkedIn for pioneering this approach - read Jay Kreps' manifesto on [The Log](http://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying) for full background.  This project aims to provide a simple starting point for many of their ideas by leveraging Amazon Web Services to simplify the deployment and maintenence of distributed infrastructure.

### features
 - streaming binary log listener for MySQL
 - emitter to Amazon Kinesis
 - consumer for writing log data to Amazon S3

### todo
 - listeners for PostgreSQL, MongoDB, (others)
 - stronger schema-ing of data


## usage for mysql

```bash
lein run -m plainview.producer -f mysql-bin.000001 -n 8779 -P 5001 -u replication-user -p password -s kinesis-stream-name
lein run -m plainview.consumer -a app-name -b bucket-name -s kinesis-stream-name
```

in your my.cnf:

```
server-id = 1234
log-bin=mysql-bin
binlog_format=row
```
