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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.uber.marmaray.common.AvroPayload;
import com.uber.marmaray.common.configuration.CassandraSinkConfiguration;
import com.uber.marmaray.common.converters.data.CassandraSinkCQLDataConverter;
import com.uber.marmaray.common.data.RDDWrapper;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.metrics.DataFeedMetricNames;
import com.uber.marmaray.common.schema.cassandra.CassandraSinkSchemaManager;
import com.uber.marmaray.utilities.StringTypes;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;

import java.util.HashMap;
import java.util.Map;

/**
 * {@link CassandraClientSink} implements the {@link CassandraSink} interface for a Cassandra sink. The AvroPayload RDD
 * will be forked into valid & invalid records based on set criteria.
 *
 * Valid records will then be written to the Cassandra backend
 */
@Slf4j
public class CassandraClientSink extends CassandraSink {

    private final CassandraSinkCQLDataConverter converter;

    public CassandraClientSink(@NonNull final CassandraSinkCQLDataConverter converter,
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
        converter.setKeyspaceName(conf.getKeyspace());
        converter.setTableName(conf.getTableName());
        final RDDWrapper<Statement> payloadWrapper = this.converter.map(data);

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

        // TODO: Figure out how to calculate the data size generated from CQL statements.
        if (this.tableMetrics.isPresent()) {
            final Map<String, String> tags = new HashMap<>();
            tags.put(TABLE_NAME_TAG, this.conf.getKeyspace() + StringTypes.UNDERSCORE + this.conf.getTableName());
            this.tableMetrics.get()
                    .createLongMetric(DataFeedMetricNames.OUTPUT_ROWCOUNT, payloadWrapper.getCount(), tags);
        }

        final String clusterName = this.conf.getClusterName();
        final String keyspaceName = this.conf.getKeyspace();

        JavaRDD<Statement> writtenRdd = payloadWrapper.getData().mapPartitions(iter -> {
                final Cluster.Builder builder = Cluster.builder().withClusterName(clusterName);
                if (this.conf.getNativePort().isPresent()) {
                    builder.withPort(Integer.parseInt(this.conf.getNativePort().get()));
                } else {
                    builder.withPort(Integer.parseInt(CassandraSinkConfiguration.DEFAULT_OUTPUT_NATIVE_PORT));
                }
                this.conf.getInitialHosts().forEach(builder::addContactPoint);

                try (final Cluster cluster = builder.build();
                     final Session session = cluster.connect(keyspaceName)) {
                    while (iter.hasNext()) {
                        Statement statement = iter.next();
                        try {
                            session.execute(statement);
                        } catch (Exception e) {
                            log.error("Exception: {}", e);
                            throw new JobRuntimeException(e);
                        }
                    }
                }
                return iter;
            }
        );
        writtenRdd.collect();

        log.info("Finished write process");
    }
}
