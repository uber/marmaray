## ParquetToCassandraJob

This job demonstrates the ability to load parquet data from HDFS 
(can be underlying a hive table or just raw parquet files with the same schema) to a cassandra cluster.

Requirements:
1. hadoop
2. spark 2.1
3. cassandra 

How to run: 

1. Create parquet files on HDFS. Can be done in spark shell:
```
val testDF = Seq( (10, "foo"), (8, "bar"), (19, "baz")).toDF("id", "name")
testDF.coalesce(1).write.format("parquet").parquet("/path/to/testParquet")
```

2. replace guava in spark (guava 19.0). Can be done in spark jars directly, or use spark.yarn.archive to update the libraries used.

3. create the following config file, and put in HDFS
```
marmaray:
  cassandra:
    cluster_name: testcluster
    datacenter: solo
    keyspace: marmaray
    partition_keys: id
    tablename: test_parquet_cassandra
  error_table:
    enabled: false
  hadoop:
    yarn_queue: default
    cassandra:
      output.thrift.address: localhost
  hive:
    data_path: /path/to/testParquet
    job_name: testParquetToCassandra
  lock_manager:
    is_enabled: false
    zk_base_path: /hoodie/no-op
  metadata_manager:
    cassandra:
      cluster: testcluster
      keyspace: marmaray
      table_name: marmaray_metadata_table
      username:
      password:
      output.thrift.address: localhost
    type: CASSANDRA
    job_name: testParquetToCassandra
  zookeeper:
    port: 2181
    quorum: unused
```

4. Run the spark job
```
./bin/spark-submit --class com.uber.marmaray.examples.job.ParquetToCassandraJob path/to/marmaray-1.0-SNAPSHOT-jar-with-dependencies.jar  -c path/to/test.yaml
```

5. On success, the data will be dispersed to cassandra. You can use CQL to verify
```
cqlsh> select * from marmaray.test_parquet_cassandra;

 id | name
----+------
 10 |  foo
 19 |  baz
  8 |  bar

(3 rows)
```
