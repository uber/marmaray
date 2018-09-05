/*
 * Copyright (c) 2018 Uber Technologies, Inc.
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions
 * of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 */
package com.uber.marmaray.common.sinks.cassandra;

import com.google.common.base.Optional;
import com.uber.marmaray.common.AvroPayload;
import com.uber.marmaray.common.configuration.CassandraSinkConfiguration;
import com.uber.marmaray.common.converters.data.CassandraSinkDataConverter;
import com.uber.marmaray.common.data.RDDWrapper;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.metrics.DataFeedMetricNames;
import com.uber.marmaray.common.metrics.CassandraPayloadRDDSizeEstimator;
import com.uber.marmaray.common.schema.cassandra.CassandraPayload;
import com.uber.marmaray.common.schema.cassandra.CassandraSinkSchemaManager;
import com.uber.marmaray.utilities.StringTypes;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.hadoop.cql3.CqlBulkOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link CassandraSSTableSink} extends the {@link CassandraSink} class for a Cassandra sink. The AvroPayload RDD
 * will be forked into valid & invalid records based on set criteria.
 *
 * Valid records will then be written to the Cassandra backend
 */
@Slf4j
public class CassandraSSTableSink extends CassandraSink {

    static {
        Config.setClientMode(true);
    }

    private static final String TMP_FILE_PATH = new Path(File.separator + "tmp",
            "dispersal" + System.nanoTime()).toString();
    private static final Class keyClass = ByteBuffer.class;
    private static final Class outputFormatClass = CqlBulkOutputFormat.class;
    private final CassandraSinkDataConverter converter;

    public CassandraSSTableSink(@NonNull final CassandraSinkDataConverter converter,
                                @NonNull final CassandraSinkSchemaManager schemaManager,
                                @NonNull final CassandraSinkConfiguration conf) {
        super(schemaManager, conf);
        this.converter = converter;
    }

    @Override
    public void write(@NonNull final JavaRDD<AvroPayload> data) {
        final Configuration hadoopConf = this.conf.getHadoopConf();

        log.info("Setting up Cassandra Table");
        this.setupCassandraTable(hadoopConf);

        log.info("Converting data to cassandra payload");
        final RDDWrapper<CassandraPayload> payloadWrapper = this.converter.map(data);

        if (payloadWrapper.getCount() == 0) {
            /*
             * As a safeguard and precaution, we fail the job if no records are dispersed.  The root cause can be
             * something innocuous like an accidental empty dataset.  But this is to explicitly protect us against
             * any data converter bugs that can take valid data and not convert it correctly resulting in an
             * incorrect error record.
             *
             * For dispersal this is ok since we assume the data that existed in Hive has been ingested and conforms
             * to a schema.  The main trade-off here is if there really is an empty dataset we cannot proceed until
             * the customer has deleted that Hive partition.  We believe that is much more preferable than a silent
             * failure and thinking the dispersal job has succeeded when no data has actually been dispersed and
             * checkpoints have been persisted to indicate the partition was successfully dispersed.
             */
            final String errMsg = String.join("No data was found to disperse.  As a safeguard, we are failing "
                    , "the job explicitly. Please check your data and ensure records are valid and "
                    , "partition and clustering keys are populated.  If your partition has empty data you will have to "
                    , "delete it to proceed.  Otherwise, please contact your support team for troubleshoooting");
            throw new JobRuntimeException(errMsg);
        }

        if (this.tableMetrics.isPresent()) {
            final Map<String, String> tags = new HashMap<>();
            tags.put(TABLE_NAME_TAG, this.conf.getKeyspace() + StringTypes.UNDERSCORE + this.conf.getTableName());
            this.tableMetrics.get()
                    .createLongMetric(DataFeedMetricNames.OUTPUT_ROWCOUNT, payloadWrapper.getCount(), tags);

            final CassandraPayloadRDDSizeEstimator cassandraPayloadRddSizeEstimator =
                    new CassandraPayloadRDDSizeEstimator();
            this.tableMetrics.get()
                    .createLongMetric(DataFeedMetricNames.OUTPUT_BYTE_SIZE,
                            cassandraPayloadRddSizeEstimator.estimateTotalSize(payloadWrapper),
                            tags);
        }

        final JavaRDD<CassandraPayload> cassandraRecords = payloadWrapper.getData();

        log.info("Initializing system properties");
        this.initSystemProperties(cassandraRecords);

        log.info("Mapping cassandra payloads to ByteBuffer RDD values");
        /*
         * Need to transform the Cassandra payload here to a kv format of bytebuffer -> List<ByteBuffer> mapping here
         * so it can be persisted to Cassandra.  The key here doesn't actually matter so a dummy value is used
         * because although the Hadoop API uses key & value the Cassandra bulk writer only needs the value.  The
         * key for the Cassandra row is implicitly derived from the defined primary key in the schema.
         */
        final JavaPairRDD<ByteBuffer, List> byteBufferRDDs =
                cassandraRecords.mapToPair(f -> new Tuple2<>(generateDummyValue(), f.convertData()));

        log.info("Creating and saving sstables");
        byteBufferRDDs.saveAsNewAPIHadoopFile(TMP_FILE_PATH,
                keyClass,
                // Due to type erasure of parameterized types just use List here
                List.class,
                outputFormatClass,
                hadoopConf);

        log.info("Finished write process");
    }

    private void initSystemProperties(@NonNull final JavaRDD<CassandraPayload> cassandraRecords) {
        if (this.conf.getStoragePort().isPresent() || this.conf.getSSLStoragePort().isPresent()) {
            final Optional<String> storagePort = this.conf.getStoragePort();
            final Optional<String> sslStoragePort = this.conf.getSSLStoragePort();

            cassandraRecords.foreach(cr -> {
                    if (storagePort.isPresent()) {
                        System.setProperty("cassandra.storage_port", storagePort.get());
                    }

                    if (sslStoragePort.isPresent()) {
                        System.setProperty("cassandra.ssl_storage_port", sslStoragePort.get());
                    }
                });
        }
    }

    /**
     * This returns a dummy value to conform with Hadoop API but isn't used for Cassandra writing
     */
    private ByteBuffer generateDummyValue() {
        return Int32Type.instance.decompose(1);
    }
}
