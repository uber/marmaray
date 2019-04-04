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
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.converters.data.CassandraSinkDataConverter;
import com.uber.marmaray.common.converters.schema.CassandraSchemaConverter;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.exceptions.MissingPropertyException;
import com.uber.marmaray.common.schema.cassandra.CassandraDataField;
import com.uber.marmaray.common.schema.cassandra.CassandraPayload;
import com.uber.marmaray.common.schema.cassandra.CassandraSchema;
import com.uber.marmaray.common.schema.cassandra.CassandraSinkSchemaManager;
import com.uber.marmaray.common.schema.cassandra.ClusterKey;
import com.uber.marmaray.common.util.AvroPayloadUtil;
import com.uber.marmaray.common.util.CassandraTestConstants;
import com.uber.marmaray.common.util.CassandraTestUtil;
import com.uber.marmaray.utilities.ByteBufferUtil;
import com.uber.marmaray.utilities.ErrorExtractor;
import com.uber.marmaray.utilities.StringTypes;
import com.uber.marmaray.utilities.TimestampInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.cassandra.hadoop.cql3.CqlBulkRecordWriter;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.uber.marmaray.common.util.CassandraTestConstants.CONFIGURATION;
import static com.uber.marmaray.common.util.CassandraTestConstants.KEY_SPACE;
import static com.uber.marmaray.common.util.CassandraTestConstants.TABLE;

@Slf4j
public class TestCassandraSSTableSink extends TestCassandraSinkUtil {

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
        final Configuration conf = new Configuration(CONFIGURATION);
        conf.setProperty(CassandraSinkConfiguration.MAX_BATCH_SIZE_MB, "1");

        final CassandraSinkDataConverter converter =
                new CassandraSinkDataConverter(avroSchema,
                        conf,
                        Optional.of(new HashSet<>(schemaFields)),
                        Optional.absent(),
                        requiredFields,
                        TimestampInfo.generateEmptyTimestampInfo(), new ErrorExtractor());

        final CassandraSchemaConverter schemaConverter = new CassandraSchemaConverter(KEY_SPACE, TABLE, Optional.absent());
        final CassandraSchema cassandraSchema = schemaConverter.convertToExternalSchema(avroSchema);

        final CassandraSinkSchemaManager schemaManager =
                new CassandraSinkSchemaManager(cassandraSchema, partitionKeys, clusteringKeys, Optional.absent());
        final CassandraSinkConfiguration sinkConf = initializeConfiguration(false, includeTimestamp);

        final CassandraSSTableSink sink = new CassandraSSTableSink(converter, schemaManager, sinkConf);
        try {
            sink.write(testData);
        } catch (Exception e) {
            Assert.assertEquals(SparkException.class, e.getClass());
            Assert.assertEquals(JobRuntimeException.class, e.getCause().getClass());
            Assert.assertEquals(MissingPropertyException.class, e.getCause().getCause().getClass());
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
        final TimestampInfo tsInfo = new TimestampInfo(timestamp, true,
          CassandraSinkConfiguration.DEFAULT_DISPERSAL_TIMESTAMP_FIELD_NAME);

        final CassandraSinkDataConverter dataconverter =
                new CassandraSinkDataConverter(AvroPayloadUtil.getAvroTestDataSchema(StringTypes.EMPTY),
                        CONFIGURATION,
                        Optional.of(new HashSet<>(schemaFields)),
                        Optional.absent(),
                        requiredFields,
                        tsInfo, new ErrorExtractor());

        final CassandraSchemaConverter schemaConverter = new CassandraSchemaConverter(KEY_SPACE, TABLE, tsInfo, Optional.absent());
        final CassandraSchema schema = schemaConverter.convertToExternalSchema(
                AvroPayloadUtil.getAvroTestDataSchema(StringTypes.EMPTY));

        final Optional<Long> ttl = Optional.of(10000L);

        final CassandraSinkSchemaManager schemaManager =
                new CassandraSinkSchemaManager(schema, partitionKeys, clusteringKeys, ttl, Optional.absent(), Optional.absent(), false);
        final CassandraSinkConfiguration conf = initializeConfiguration(false, addLongTimestamp);

        final CassandraSSTableSink sink = new CassandraSSTableSink(dataconverter, schemaManager, conf);
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
        final TimestampInfo tsInfo = new TimestampInfo(timestamp, false,
          CassandraSinkConfiguration.DEFAULT_DISPERSAL_TIMESTAMP_FIELD_NAME);

        final CassandraSinkDataConverter converter =
                new CassandraSinkDataConverter(
                        avroSchema,
                        CONFIGURATION,
                        Optional.of(new HashSet<>(schemaFields)),
                        Optional.absent(),
                        requiredFields,
                        tsInfo, new ErrorExtractor());

        final CassandraSchemaConverter schemaConverter = new CassandraSchemaConverter(KEY_SPACE, TABLE, tsInfo, Optional.absent());
        final CassandraSchema schema = schemaConverter.convertToExternalSchema(avroSchema);

        final CassandraSinkSchemaManager schemaManager =
                new CassandraSinkSchemaManager(schema, partitionKeys, clusteringKeys, Optional.absent());
        final CassandraSinkConfiguration conf = initializeConfiguration(true, addStringTimestamp);

        final CassandraSSTableSink sink = new CassandraSSTableSink(converter, schemaManager, conf);
        sink.write(testData);
        validateCassandraTable(100, true, addStringTimestamp);
    }

    @Test
    public void testIgnoredHosts() {
        final Configuration conf = new Configuration();
        conf.setProperty(CassandraSinkConfiguration.ENABLE_IGNORE_HOSTS, "true");
        conf.setProperty(CassandraSinkConfiguration.KEYSPACE, "keyspace");
        conf.setProperty(CassandraSinkConfiguration.TABLE_NAME, "table");
        conf.setProperty(CassandraSinkConfiguration.CLUSTER_NAME, "cluster");
        conf.setProperty(CassandraSinkConfiguration.PARTITION_KEYS, "partition");
        conf.setProperty(CassandraSinkConfiguration.DATACENTER, "dc");
        CassandraSinkConfiguration sinkConf = new CassandraSinkConfiguration(conf);
        CassandraSSTableSink sink = Mockito.spy(new CassandraSSTableSink(
                Mockito.mock(CassandraSinkDataConverter.class),
                Mockito.mock(CassandraSinkSchemaManager.class),
                sinkConf));
        final String ignoredHosts = "127.0.0.1";
        Mockito.when(sink.computeIgnoredHosts()).thenReturn(ignoredHosts);
        org.apache.hadoop.conf.Configuration hadoopConf = sinkConf.getHadoopConf();
        Assert.assertNull(hadoopConf.get(CqlBulkRecordWriter.IGNORE_HOSTS));
        sink.setIgnoredHosts(hadoopConf);
        Assert.assertEquals(ignoredHosts, hadoopConf.get(CqlBulkRecordWriter.IGNORE_HOSTS));
        // test that nothing happens if the flag is disabled
        conf.setProperty(CassandraSinkConfiguration.ENABLE_IGNORE_HOSTS, "false");
        sinkConf = new CassandraSinkConfiguration(conf);
        hadoopConf = sinkConf.getHadoopConf();
        sink = Mockito.spy(new CassandraSSTableSink(
                Mockito.mock(CassandraSinkDataConverter.class),
                Mockito.mock(CassandraSinkSchemaManager.class),
                sinkConf));
        Assert.assertNull(hadoopConf.get(CqlBulkRecordWriter.IGNORE_HOSTS));
        sink.setIgnoredHosts(hadoopConf);
        Assert.assertNull(hadoopConf.get(CqlBulkRecordWriter.IGNORE_HOSTS));

    }

    @Test
    public void testBatching() {
        CassandraSinkConfiguration sinkConf = initializeConfiguration(false, false);
        final Configuration conf = sinkConf.getConf();
        conf.setProperty(CassandraSinkConfiguration.MAX_BATCH_SIZE_MB, "1");
        sinkConf = new CassandraSinkConfiguration(conf);

        final Schema schema = SchemaBuilder.builder().record("myrecord").fields()
                .name(INT_FIELD).type().nullable().intType().noDefault()
                .name(STRING_FIELD).type().nullable().stringType().noDefault()
                .endRecord();
        final List<AvroPayload> payloadList = IntStream.range(1, 5).mapToObj(
                i -> {
                    final GenericRecord gr = new GenericData.Record(schema);
                    gr.put(INT_FIELD, i);
                    gr.put(STRING_FIELD, String.valueOf(i));
                    final AvroPayload payload = new AvroPayload(gr);
                    return payload;
                }
        ).collect(Collectors.toList());
        final JavaRDD<AvroPayload> data = this.jsc.get().parallelize(payloadList);
        final CassandraSinkDataConverter converter = new CassandraSinkDataConverter(schema, conf, Optional.absent(),
                Optional.absent(), Collections.singletonList(INT_FIELD), TimestampInfo.generateEmptyTimestampInfo(),
                new ErrorExtractor());
        final CassandraSchemaConverter cassandraSchemaConverter = new CassandraSchemaConverter(
                sinkConf.getKeyspace(), sinkConf.getTableName(),
                TimestampInfo.generateEmptyTimestampInfo(), sinkConf.getFilteredColumns());
        final CassandraSchema cassandraSchema = cassandraSchemaConverter.convertToExternalSchema(schema);

        final CassandraSinkSchemaManager manager = new CassandraSinkSchemaManager(cassandraSchema,
                Collections.singletonList(INT_FIELD), Collections.emptyList(), Optional.absent(), Optional.absent(),
                Optional.absent(), false);
        final CassandraSSTableSink sink = new CassandraSSTableSink(converter, manager, sinkConf);
        sink.write(data);
        validateCassandraTable(4, true, false);
    }

    @Test
    public void testCountComputation() {
        final CassandraSinkConfiguration mockConfiguration = Mockito.mock(CassandraSinkConfiguration.class);
        final CassandraSSTableSink mockSink = Mockito.spy(
                new CassandraSSTableSink(Mockito.mock(CassandraSinkDataConverter.class),
                        Mockito.mock(CassandraSinkSchemaManager.class),
                        mockConfiguration));
        Mockito.when(mockConfiguration.getMaxBatchSizeMb()).thenReturn(1L);
        Mockito.when(mockConfiguration.isBatchEnabled()).thenReturn(true);
        // test round up
        Assert.assertEquals(2, mockSink.getNumBatches(1024 * 1024 * 1.5));
        // test exact value
        Assert.assertEquals(1, mockSink.getNumBatches(1024 * 1024));
        Mockito.when(mockConfiguration.getMaxBatchSizeMb()).thenReturn(Long.MAX_VALUE);
        Assert.assertEquals(1, mockSink.getNumBatches(1));
    }

    @Test
    public void testComputeSortOrder() {
        final CassandraSinkConfiguration mockConfiguration = Mockito.mock(CassandraSinkConfiguration.class);
        final CassandraSSTableSink sink = new CassandraSSTableSink(Mockito.mock(CassandraSinkDataConverter.class),
                        Mockito.mock(CassandraSinkSchemaManager.class),
                        mockConfiguration);
        Mockito.when(mockConfiguration.getPartitionKeys()).thenReturn(Arrays.asList("e", "a"));
        Mockito.when(mockConfiguration.getClusteringKeys()).thenReturn(Collections.singletonList(new ClusterKey("b", ClusterKey.Order.ASC)));
        final CassandraPayload payload = new CassandraPayload();
        payload.addField(new CassandraDataField(ByteBufferUtil.wrap("a"), ByteBufferUtil.wrap("aVal")));
        payload.addField(new CassandraDataField(ByteBufferUtil.wrap("b"), ByteBufferUtil.wrap("bVal")));
        payload.addField(new CassandraDataField(ByteBufferUtil.wrap("c"), ByteBufferUtil.wrap("cVal")));
        payload.addField(new CassandraDataField(ByteBufferUtil.wrap("d"), ByteBufferUtil.wrap("dVal")));
        payload.addField(new CassandraDataField(ByteBufferUtil.wrap("e"), ByteBufferUtil.wrap("eVal")));
        payload.addField(new CassandraDataField(ByteBufferUtil.wrap("f"), ByteBufferUtil.wrap("fVal")));
        final List<Integer> sortOrder = sink.computeSortOrder(payload);
        Assert.assertEquals(3, sortOrder.size());
        Assert.assertEquals(4, (int) sortOrder.get(0));
        Assert.assertEquals(0, (int) sortOrder.get(1));
        Assert.assertEquals(1, (int) sortOrder.get(2));

    }
}
