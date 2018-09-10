# JSON to Hudi example

This demonstrates the ability to load JSON files to a Hudi-based table.

# Instructions
1. `mvn clean package` to obtain `marmaray-VERSION-jar-with-dependencies.jar`
2. Copy the file to spark
3. Create a directory to hold the files, and replace BASE_PATH in sampleConfig.yaml with that path. For example, I used oss_test as my base path.
4. Create the schema service path (BASE_PATH/schema) and put the exampleSchema.1.avsc and errorSchema.1.avsc files in it
5. Create the data path (BASE_PATH/data) and put the exampleData.json in it
6. Put the yaml file somewhere in BASE_PATH
Call spark-submit with `spark-submit --class com.uber.marmaray.common.job.JsonHoodie marmaray-VERSION-jar-with-dependencies.jar -c BASE_PATH/exampleConf.yaml`

It will create two hoodie output data sets, one with two successfully ingested records (BASE_PATH/table), one with two error records (BASE_PATH/errorTable).

If you look into the error records, you can see that one record is failing due to missing a required field, timestamp, and the other is failing due to having a schema mismatch.

```
scala> spark.read.parquet("oss_test/table/2018/09/09/61ee3dbd-7c97-42c1-bb1f-2d9a50c03d5c_0_20180910020542.parquet").show(false)
+-------------------+--------------------+------------------+----------------------+--------------------------------------------------------------------------------+-------------+--------+------------------------------+----------+
|_hoodie_commit_time|_hoodie_commit_seqno|_hoodie_record_key|_hoodie_partition_path|_hoodie_file_name                                                               |firstName    |lastName|address                       |timestamp |
+-------------------+--------------------+------------------+----------------------+--------------------------------------------------------------------------------+-------------+--------+------------------------------+----------+
|20180910020542     |20180910020542_0_3  |Foo               |2018/09/09            |2018-09-09_61ee3dbd-7c97-42c1-bb1f-2d9a50c03d5c_0_20180910020542_27_3888.parquet|Foo          |Bar     |[12345 Main St,Anytown,123456]|1536518565|
|20180910020542     |20180910020542_0_4  |OnlyFirstname     |2018/09/09            |2018-09-09_61ee3dbd-7c97-42c1-bb1f-2d9a50c03d5c_0_20180910020542_27_3888.parquet|OnlyFirstname|null    |null                          |1536518566|
+-------------------+--------------------+------------------+----------------------+--------------------------------------------------------------------------------+-------------+--------+------------------------------+----------+

scala> spark.read.parquet("oss_test/errorTable/2018/09/10/06af2c95-0d3e-43e8-9f5e-717dd9b20209_0_20180910020527.parquet").show(false)
+-------------------+--------------------+--------------------------+----------------------+----------------------------------------------------------------------------+-----------------+-------------------------------------------------------------------------------------------------+----------------------------------------------------------+---------------------------+----------------------------------+
|_hoodie_commit_time|_hoodie_commit_seqno|_hoodie_record_key        |_hoodie_partition_path|_hoodie_file_name                                                           |hadoop_row_key   |hadoop_error_source_data                                                                         |hadoop_error_exception                                    |hadoop_changelog_columns   |hadoop_application_id             |
+-------------------+--------------------+--------------------------+----------------------+----------------------------------------------------------------------------+-----------------+-------------------------------------------------------------------------------------------------+----------------------------------------------------------+---------------------------+----------------------------------+
|20180910020527     |20180910020527_0_1  |Hoodie_record_key_constant|2018/09/10            |2018-09-10_06af2c95-0d3e-43e8-9f5e-717dd9b20209_0_20180910020527_2_2.parquet|ROW_KEY_NOT_FOUND|RawData(data={"firstName": "badData", "address": {"zip": "NotANumber"}, "timestamp": 1536518567})|Type conversion error for field zip, NotANumber for "long"|CHANGELOG_COLUMNS_NOT_FOUND|application_1525479763952_10243196|
+-------------------+--------------------+--------------------------+----------------------+----------------------------------------------------------------------------+-----------------+-------------------------------------------------------------------------------------------------+----------------------------------------------------------+---------------------------+----------------------------------+


scala> spark.read.parquet("oss_test/errorTable/2018/09/10/de94d50f-2c07-405d-9448-437cbf686343_0_20180910020538.parquet").show(false)
+-------------------+--------------------+--------------------------+----------------------+--------------------------------------------------------------------------------+-----------------+---------------------------------------------------------------------------------------+-----------------------------------+---------------------------+----------------------------------+
|_hoodie_commit_time|_hoodie_commit_seqno|_hoodie_record_key        |_hoodie_partition_path|_hoodie_file_name                                                               |hadoop_row_key   |hadoop_error_source_data                                                               |hadoop_error_exception             |hadoop_changelog_columns   |hadoop_application_id             |
+-------------------+--------------------+--------------------------+----------------------+--------------------------------------------------------------------------------+-----------------+---------------------------------------------------------------------------------------+-----------------------------------+---------------------------+----------------------------------+
|20180910020538     |20180910020538_0_2  |Hoodie_record_key_constant|2018/09/10            |2018-09-10_de94d50f-2c07-405d-9448-437cbf686343_0_20180910020538_14_1433.parquet|ROW_KEY_NOT_FOUND|{"firstName": "missingTimestamp", "lastName": null, "address": null, "timestamp": null}|required field is missing:timestamp|CHANGELOG_COLUMNS_NOT_FOUND|application_1525479763952_10243196|
+-------------------+--------------------+--------------------------+----------------------+--------------------------------------------------------------------------------+-----------------+---------------------------------------------------------------------------------------+-----------------------------------+---------------------------+----------------------------------+```
```
