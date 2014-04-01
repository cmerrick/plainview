## plainview

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
 - process for capturing an initial state of the data

### use with mysql

How to use plainview to listen to your MySQL replication stream and write raw change data to Amazon S3.

##### 1. Setup your MySQL Server
Your server needs to be configured as a replication master using the "row" binary log format.  Set
the following in your server's my.cnf file:

```
log-bin=mysql-bin
binlog_format=row
server-id=1234
```

Where `log-bin` specifies the name to use to store binary log files, and `server-id` is a unique number to identify this server in the replication system.  Restart your mysql server to put the changes into effect.

Finally, create a MySQL replication user:

```sql
mysql> GRANT REPLICATION SLAVE ON *.* TO '<repl-user>'@'192.168.%' IDENTIFIED BY '<repl-pass>';
```

Modify with an appropriate username and password, and replace `192.168.%` with the network address or subnet that you'll be connecting from.  Finally, grab the current position of the replication log:

```bash
mysql> show master status;
+------------------+-----------+--------------+------------------+
| File             | Position  | Binlog_Do_DB | Binlog_Ignore_DB |
+------------------+-----------+--------------+------------------+
| mysql-bin.000019 | 480798760 |              |                  |
+------------------+-----------+--------------+------------------+
```

##### 2. Setup a Kinesis Stream

(TODO)

##### 3. Run the plainview producer

plainview's producer listens to your MySQL server's replication system and forwards data to the Kinesis stream you just setup.

Clone this repository and run the following in your shell to give plainview access to the Kinesis stream you just setup:

```
export AWS_ACCESS_KEY=<your-key>
export AWS_SECRET_KEY=<your-secret>
```

The key will need full permissions on Kinesis.  From the same shell, run the producer

```bash
lein run -m plainview.producer -i <server-id> -f <mysql-file> -n <mysql-position> -P <mysql-port> -u <repl-user> -p <repl-pass> -s <kinesis-stream>
```

Where
 - `<server-id>` is the unique ID from your master MySQL server's my.cnf file
 - `<mysql-file>` is the "File" that your master MySQL server reported in Step 1. (in this case, `mysql-bin.000019`)
 - `<mysql-position>` is the "Position" that your master MySQL server reported in Step 1. (in this case, `480798760`)
 - `<mysql-port>` is the port of your master MySQL server
 - `<repl-user>` and `<repl-pass>` are the credentials you GRANTed permission to in Step 1.
 - `<kinesis-stream>` is the name of the Kinesis stream from Step 2.

The producer will run continuously, listening for updates coming through replication.

##### 4. Run the plainview consumer

plainview's consumer listens to a Kinesis stream for data from the producer and writes it to Amazon S3.

Again run the following to give the code access to Amazon resources:

```
export AWS_ACCESS_KEY=<your-key>
export AWS_SECRET_KEY=<your-secret>
```

This key will need read permission on Kinesis, full permission on DynamoDB, and permission to write to an S3 bucket.  From the same shell, run the consumer:

```bash
lein run -m plainview.consumer -a <kinesis-app> -b <s3-bucket> -s <kinesis-stream>
```

Where
 - `<kinesis-app>` is a unique name for this consumer.  Kinesis uses this name to maintain the consumer's state.
 - `<s3-bucket>` is the S3 bucket to write to
 - `<kinesis-stream>` is the name fo the Kinesis stream from Step 2.
