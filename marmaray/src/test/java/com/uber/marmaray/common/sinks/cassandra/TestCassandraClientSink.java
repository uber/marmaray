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

import static com.uber.marmaray.common.util.CassandraTestConstants.KEY_SPACE;
import static com.uber.marmaray.common.util.CassandraTestConstants.TABLE;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.uber.marmaray.common.AvroPayload;
import com.uber.marmaray.common.configuration.CassandraSinkConfiguration;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.converters.data.CassandraSinkCQLDataConverter;
import com.uber.marmaray.common.converters.data.CassandraSinkDataConverter;
import com.uber.marmaray.common.converters.schema.CassandraSchemaConverter;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.schema.cassandra.CassandraSchema;
import com.uber.marmaray.common.schema.cassandra.CassandraSinkSchemaManager;
import com.uber.marmaray.common.schema.cassandra.ClusterKey;
import com.uber.marmaray.common.util.AbstractSparkTest;
import com.uber.marmaray.common.util.AvroPayloadUtil;
import com.uber.marmaray.common.util.CassandraTestConstants;
import com.uber.marmaray.common.util.CassandraTestUtil;
import com.uber.marmaray.utilities.ErrorExtractor;
import com.uber.marmaray.utilities.SchemaUtil;
import com.uber.marmaray.utilities.StringTypes;
import com.uber.marmaray.utilities.TimestampInfo;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

@Slf4j
public class TestCassandraClientSink extends TestCassandraSinkUtil {

    private static final String TEST_TIMESTAMP = "10000";

    @Before
    public void setupTest() {
        super.setupTest();
        try {
            CassandraTestUtil.setupCluster();
        } catch (TTransportException | IOException e) {
            throw new JobRuntimeException("Error while initializing Cassandra cluster for test: " + e.getMessage());
        }
    }

    @After
    public void teardownTest() {
        super.teardownTest();
        CassandraTestUtil.teardownCluster();
    }

    @Test
    public void testWriteAllFieldsMockDataToCassandraWithoutTimestamp() {
        testWriteAllFieldsMockDataToCassandra(false);
    }

    @Test
    public void testWriteAllFieldsMockDataToCassandraWithTimestamp() {
        testWriteAllFieldsMockDataToCassandra(true);
    }

    /**
     * We exclude the boolean field from the input schema and don't write it to Cassandra but do add a timestamp
     */
    @Test
    public void testWriteOnlySpecifiedFieldsMockDataToCassandraWithTimestamp() {
        testWriteOnlySpecifiedFieldsMockDataToCassandra(true);
    }

    /**
     * We exclude the boolean field from the input schema and don't write it to Cassandra and do NOT add a timestamp
     */
    @Test
    public void testWriteOnlySpecifiedFieldsMockDataToCassandraWithoutTimestamp() {
        testWriteOnlySpecifiedFieldsMockDataToCassandra(false);
    }

    /*
     * In this test, each Avro Payload is missing one of the partition keys in the Cassandra Schema
     * The Avro Schema declares the field which is the partition key but does not have any data for it.
     * Everything should be forked as an error record so no data is written to Cassandra and the job fails
     */
    @Test
    public void testWriteMockErrorDataToCassandra() {
        final boolean includeTimestamp = false;
        final JavaRDD<AvroPayload> testData = AvroPayloadUtil.generateTestData(this.jsc.get(),
                100,
                CassandraTestConstants.INT_FIELD);

        final List<String> partitionKeys = Collections.singletonList(CassandraTestConstants.INT_FIELD);
        final List<ClusterKey> clusteringKeys = Collections.singletonList(
                new ClusterKey(CassandraTestConstants.STRING_FIELD, ClusterKey.Order.DESC));

        final List<String> schemaFields = AvroPayloadUtil.getSchemaFields();
        final List<String> requiredFields = Arrays.asList(CassandraTestConstants.INT_FIELD,
                CassandraTestConstants.STRING_FIELD);

        // Still want to include the INT field in the schema but we don't write data for it
        final Schema avroSchema = AvroPayloadUtil.getAvroTestDataSchema(StringTypes.EMPTY);

        final CassandraSinkCQLDataConverter converter =
                new CassandraSinkCQLDataConverter(avroSchema,
                        new Configuration(),
                        Optional.of(new HashSet<>(schemaFields)),
                        requiredFields,
                        TimestampInfo.generateEmptyTimestampInfo(), new ErrorExtractor());

        final CassandraSchemaConverter schemaConverter = new CassandraSchemaConverter(KEY_SPACE, TABLE, Optional.absent());
        final CassandraSchema cassandraSchema = schemaConverter.convertToExternalSchema(avroSchema);

        final CassandraSinkSchemaManager schemaManager =
                new CassandraSinkSchemaManager(cassandraSchema, partitionKeys, clusteringKeys);

        final CassandraSinkConfiguration conf = initializeConfiguration(false, includeTimestamp);
        final CassandraClientSink sink = new CassandraClientSink(converter, schemaManager, conf);
        try {
            sink.write(testData);
        } catch (Exception e) {
            Assert.assertEquals(SparkException.class, e.getClass());
            Assert.assertEquals(JobRuntimeException.class, e.getCause().getClass());
            Assert.assertEquals(JobRuntimeException.class, e.getCause().getCause().getClass());
        }
    }

    private void testWriteAllFieldsMockDataToCassandra(boolean addLongTimestamp) {
        final JavaRDD<AvroPayload> testData = AvroPayloadUtil.generateTestData(this.jsc.get(),
                100,
                StringTypes.EMPTY);

        final List<String> schemaFields = AvroPayloadUtil.getSchemaFields();

        final List<String> partitionKeys = Collections.singletonList(schemaFields.get(0));
        final List<ClusterKey> clusteringKeys = Collections.singletonList(
                new ClusterKey(schemaFields.get(1), ClusterKey.Order.DESC));

        final List<String> requiredFields = Arrays.asList(schemaFields.get(0), schemaFields.get(1));

        final Optional<String> timestamp = addLongTimestamp ? Optional.of(TEST_TIMESTAMP) : Optional.absent();
        final TimestampInfo tsInfo = new TimestampInfo(timestamp, true);

        final CassandraSinkCQLDataConverter converter =
                new CassandraSinkCQLDataConverter(AvroPayloadUtil.getAvroTestDataSchema(StringTypes.EMPTY),
                        new Configuration(),
                        Optional.of(new HashSet<>(schemaFields)),
                        requiredFields,
                        tsInfo, new ErrorExtractor());

        final CassandraSchemaConverter schemaConverter = new CassandraSchemaConverter(KEY_SPACE, TABLE, tsInfo, Optional.absent());
        final CassandraSchema schema = schemaConverter.convertToExternalSchema(
                AvroPayloadUtil.getAvroTestDataSchema(StringTypes.EMPTY));

        final Optional<Long> ttl = Optional.of(10000L);

        final CassandraSinkSchemaManager schemaManager =
                new CassandraSinkSchemaManager(schema, partitionKeys, clusteringKeys, ttl);

        final CassandraSinkConfiguration conf = initializeConfiguration(false, addLongTimestamp);
        final CassandraClientSink sink = new CassandraClientSink(converter, schemaManager, conf);
        sink.write(testData);
        validateCassandraTable(100, false, addLongTimestamp);
    }

    private void testWriteOnlySpecifiedFieldsMockDataToCassandra(final boolean addStringTimestamp) {

        final JavaRDD<AvroPayload> testData = AvroPayloadUtil.generateTestData(this.jsc.get(),
                100,
                StringTypes.EMPTY);

        final List<String> schemaFields = AvroPayloadUtil.getSchemaFields(CassandraTestConstants.BOOLEAN_FIELD);

        final List<String> partitionKeys = Collections.singletonList(schemaFields.get(0));
        final List<ClusterKey> clusteringKeys = Collections.singletonList(
                new ClusterKey(schemaFields.get(1), ClusterKey.Order.DESC));

        final List<String> requiredFields = Arrays.asList(schemaFields.get(0), schemaFields.get(1));
        final Schema avroSchema = AvroPayloadUtil.getAvroTestDataSchema(CassandraTestConstants.BOOLEAN_FIELD);

        final Optional<String> timestamp = addStringTimestamp ? Optional.of(TEST_TIMESTAMP) : Optional.absent();
        final TimestampInfo tsInfo = new TimestampInfo(timestamp, false);

        final CassandraSinkCQLDataConverter converter =
                new CassandraSinkCQLDataConverter(
                        avroSchema,
                        new Configuration(),
                        Optional.of(new HashSet<>(schemaFields)),
                        requiredFields,
                        tsInfo, new ErrorExtractor());

        final CassandraSchemaConverter schemaConverter = new CassandraSchemaConverter(KEY_SPACE, TABLE, tsInfo, Optional.absent());
        final CassandraSchema schema = schemaConverter.convertToExternalSchema(avroSchema);

        final CassandraSinkSchemaManager schemaManager =
                new CassandraSinkSchemaManager(schema, partitionKeys, clusteringKeys);

        final CassandraSinkConfiguration conf = initializeConfiguration(true, addStringTimestamp);
        final CassandraClientSink sink = new CassandraClientSink(converter, schemaManager, conf);
        sink.write(testData);
        validateCassandraTable(100, true, addStringTimestamp);
    }
}
