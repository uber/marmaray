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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.primitives.Ints;
import com.uber.marmaray.common.AvroPayload;
import com.uber.marmaray.common.configuration.CassandraSinkConfiguration;
import com.uber.marmaray.common.converters.data.CassandraSinkDataConverter;
import com.uber.marmaray.common.data.ForkData;
import com.uber.marmaray.common.data.RDDWrapper;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.forkoperator.ForkFunction;
import com.uber.marmaray.common.forkoperator.ForkOperator;
import com.uber.marmaray.common.metrics.CassandraMetric;
import com.uber.marmaray.common.metrics.CassandraPayloadRDDSizeEstimator;
import com.uber.marmaray.common.metrics.DataFeedMetricNames;
import com.uber.marmaray.common.metrics.DataFeedMetrics;
import com.uber.marmaray.common.metrics.ErrorCauseTagNames;
import com.uber.marmaray.common.metrics.ModuleTagNames;
import com.uber.marmaray.common.schema.cassandra.CassandraDataField;
import com.uber.marmaray.common.schema.cassandra.CassandraPayload;
import com.uber.marmaray.common.schema.cassandra.CassandraSinkSchemaManager;
import com.uber.marmaray.common.schema.cassandra.ClusterKey;
import com.uber.marmaray.utilities.ByteBufferUtil;
import com.uber.marmaray.utilities.SizeUnit;
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
import parquet.Preconditions;
import scala.Tuple2;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.cassandra.hadoop.cql3.CqlBulkRecordWriter.IGNORE_HOSTS;

/**
 * {@link CassandraSSTableSink} extends the {@link CassandraSink} class for a Cassandra sink. The AvroPayload RDD
 * will be forked into valid & invalid records based on set criteria.
 * <p>
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
    public void setDataFeedMetrics(@NonNull final DataFeedMetrics dataFeedMetrics) {
        log.info("set datafeed metric + converter");
        super.setDataFeedMetrics(dataFeedMetrics);
        this.converter.setDataFeedMetrics(dataFeedMetrics);
    }

    @Override
    public void write(@NonNull final JavaRDD<AvroPayload> data) {
        final Configuration hadoopConf = this.conf.getHadoopConf();

        setIgnoredHosts(hadoopConf);

        log.info("Setting up Cassandra Table");
        this.setupCassandraTable(hadoopConf);

        log.info("Converting data to cassandra payload");
        final RDDWrapper<CassandraPayload> payloadWrapper;
        if (this.conf.isBatchEnabled()) {
            payloadWrapper = this.sort(this.converter.map(data));
        } else {
            payloadWrapper = this.converter.map(data);
        }

        final CassandraPayloadRDDSizeEstimator cassandraPayloadRddSizeEstimator =
                new CassandraPayloadRDDSizeEstimator();

        final long estimatedPayloadSize = cassandraPayloadRddSizeEstimator.estimateTotalSize(payloadWrapper);

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
            if (this.tableMetrics.isPresent()) {
                this.tableMetrics.get()
                        .createLongFailureMetric(DataFeedMetricNames.MARMARAY_JOB_ERROR, 1, DataFeedMetricNames
                                .getErrorModuleCauseTags(ModuleTagNames.SINK, ErrorCauseTagNames.NO_DATA));
            }
            final String errMsg = String.join(""
                    , "No data was found to disperse.  As a safeguard, we are failing "
                    , "the job explicitly. Please check your data and ensure records are valid and "
                    , "partition and clustering keys are populated.  If your partition has empty data you will have to "
                    , "delete it to proceed.  Otherwise, please contact your support team for troubleshoooting");
            throw new JobRuntimeException(errMsg);
        }

        if (this.tableMetrics.isPresent()) {
            this.tableMetrics.get()
                    .createLongMetric(DataFeedMetricNames.OUTPUT_BYTE_SIZE,
                            estimatedPayloadSize,
                            CassandraMetric.createTableNameTags(this.conf));
        }

        if (this.conf.isBatchEnabled()) {
            final int numBatches = getNumBatches(estimatedPayloadSize);
            final long recordsPerBatch = (long) Math.ceil((double) payloadWrapper.getCount() / numBatches);
            log.info("Running with {} batches of {} records", numBatches, recordsPerBatch);
            final JavaRDD<Tuple2<CassandraPayload, Long>> indexedRdd =
                    payloadWrapper.getData().zipWithIndex().map(r -> r);
            final ForkFunction<Tuple2<CassandraPayload, Long>> forkFunction =
                    new ForkFunction<Tuple2<CassandraPayload, Long>>() {
                @Override
                protected List<ForkData<Tuple2<CassandraPayload, Long>>> process(
                        final Tuple2<CassandraPayload, Long> record) {
                    return Collections.singletonList(new ForkData<>(
                            Collections.singletonList((int) (record._2().intValue() / recordsPerBatch)), record));
                }
            };
            final ForkOperator<Tuple2<CassandraPayload, Long>> forkOperator =
                    new ForkOperator<>(indexedRdd, forkFunction, this.conf.getConf());
            final List<Integer> batchList = IntStream.range(0, numBatches).boxed().collect(Collectors.toList());
            final long sleepDurationMillis = TimeUnit.SECONDS.toMillis(this.conf.getMinBatchDurationSeconds());
            forkFunction.registerKeys(batchList);
            forkOperator.execute();
            batchList.forEach(
                index -> {
                    log.info("Processing batch {}", index);
                    final long startTime = System.currentTimeMillis();
                    final JavaRDD<Tuple2<CassandraPayload, Long>> indexedFork = forkOperator.getRDD(index);
                    final JavaRDD<CassandraPayload> indexedData = indexedFork.map(Tuple2::_1);
                    saveCassandraPayloads(hadoopConf, indexedData.coalesce(1));
                    final long duration = System.currentTimeMillis() - startTime;
                    final long millisToSleep = Math.max(0, sleepDurationMillis - duration);
                    log.info("Complete, sleeping for {} ms", millisToSleep);
                    try {
                        Thread.sleep(millisToSleep);
                    } catch (InterruptedException e) {
                        throw new JobRuntimeException("Interrupted while sleeping between batches", e);
                    }
                }
            );
        } else {
            saveCassandraPayloads(hadoopConf, payloadWrapper.getData());
        }
    }

    @VisibleForTesting
    int getNumBatches(final double estimatedPayloadSize) {
        Preconditions.checkState(this.conf.isBatchEnabled(), "Batching isn't enabled; no batches to get.");
        final int numBatches =
                (int) Math.ceil(estimatedPayloadSize / SizeUnit.MEGABYTES.toBytes(this.conf.getMaxBatchSizeMb()));
        // account for large disparity in the records
        return Math.max(1, numBatches);
    }

    /*
     * Actually save the cassandra payloads per batch
     */
    private void saveCassandraPayloads(@NonNull final Configuration hadoopConf,
                               @NonNull final JavaRDD<CassandraPayload> cassandraRecords) {
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
        try {
            byteBufferRDDs.saveAsNewAPIHadoopFile(TMP_FILE_PATH,
                    keyClass,
                    // Due to type erasure of parameterized types just use List here
                    List.class,
                    outputFormatClass,
                    hadoopConf);
        } catch (Exception e) {
            if (this.tableMetrics.isPresent()) {
                this.tableMetrics.get()
                        .createLongFailureMetric(DataFeedMetricNames.MARMARAY_JOB_ERROR, 1, DataFeedMetricNames
                                .getErrorModuleCauseTags(ModuleTagNames.SINK, ErrorCauseTagNames.WRITE_TO_CASSANDRA));
            }
            log.error("Error while writing to cassandra SSTable", e);
            throw new JobRuntimeException(e);
        }
        log.info("Finished write process");
    }

    @VisibleForTesting
    void setIgnoredHosts(final Configuration hadoopConf) {
        if (this.conf.isEnableIgnoreHosts()) {
            final String ignoredHosts = computeIgnoredHosts();
            log.info("Ignoring hosts: {}", ignoredHosts);
            hadoopConf.set(IGNORE_HOSTS, ignoredHosts);
        }
    }

    /**
     * Method to compute a list of hosts to ignore, in order to not ship the SSTables to those hosts
     * @return comma-separated string of hosts
     */
    protected String computeIgnoredHosts() {
        return StringTypes.EMPTY;
    }

    private RDDWrapper<CassandraPayload> sort(@NonNull final RDDWrapper<CassandraPayload> wrapper) {
        final JavaRDD<CassandraPayload> data = wrapper.getData();
        final CassandraPayload firstRecord = data.first();
        final List<Integer> sortOrder = computeSortOrder(firstRecord);
        final JavaRDD<CassandraPayload> sortedData = data.sortBy(
            cassandraPayload -> {
                final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final List<ByteBuffer> values = cassandraPayload.convertData();
                sortOrder.forEach(
                    i -> {
                        try {
                            baos.write(values.get(i).array());
                        } catch (IOException e) {
                            throw new JobRuntimeException("Unable to produce sort key for record", e);
                        }
                    }
                );
                return ByteBuffer.wrap(baos.toByteArray());
            },
            true,
            data.getNumPartitions());
        return new RDDWrapper<>(sortedData, wrapper.getCount());
    }

    /**
     * Determine which numerical fields should be sorted, and in which order so we can quickly access the fields
     * from the CassandraPayload
     *
     * Order should be partition keys, followed by clustering keys. If the example parquet has
     * a, b, c, d, e fields, and the partition keys are c, a and the clustering keys are e, b, should return
     * {2, 0, 4, 1} so we can quickly
     * @param record sample record we can use to determine key ordering
     * @return List of indicies of the ByteBuffers to use for sorting.
     */
    @VisibleForTesting
    List<Integer> computeSortOrder(@NonNull final CassandraPayload record) {
        // map sort order -> index in fields list
        final int[] sortOrder = new int[this.conf.getPartitionKeys().size() + this.conf.getClusteringKeys().size()];
        final int partitionKeySize = this.conf.getPartitionKeys().size();
        final List<CassandraDataField> fields = record.getData();
        for (int i = 0; i < fields.size(); i++) {
            final String columnName = ByteBufferUtil.convertToString(fields.get(i).getColumnKey());
            final int partitionKeyIndex = this.conf.getPartitionKeys().indexOf(columnName);
            if (partitionKeyIndex != -1) {
                sortOrder[partitionKeyIndex] = i;
            } else {
                final List<ClusterKey> clusterKeys = this.conf.getClusteringKeys();
                for (int j = 0; j < this.conf.getClusteringKeys().size(); j++) {
                    if (clusterKeys.get(j).isClusterKeyColumn(columnName)) {
                        sortOrder[j + partitionKeySize] = i;
                        break;
                    }
                }
            }
        }
        return Ints.asList(sortOrder);
    }

    private void initSystemProperties(@NonNull final JavaRDD<CassandraPayload> cassandraRecords) {
        if (this.conf.getStoragePort().isPresent() || this.conf.getSSLStoragePort().isPresent()) {
            final Optional<String> storagePort = this.conf.getStoragePort();
            final Optional<String> sslStoragePort = this.conf.getSSLStoragePort();

            cassandraRecords.foreachPartition(
                partition -> {
                    if (storagePort.isPresent()) {
                        System.setProperty("cassandra.storage_port", storagePort.get());
                    }

                    if (sslStoragePort.isPresent()) {
                        System.setProperty("cassandra.ssl_storage_port", sslStoragePort.get());
                    }
                }
            );
        }
    }

    /**
     * This returns a dummy value to conform with Hadoop API but isn't used for Cassandra writing
     */
    private ByteBuffer generateDummyValue() {
        return Int32Type.instance.decompose(1);
    }
}
