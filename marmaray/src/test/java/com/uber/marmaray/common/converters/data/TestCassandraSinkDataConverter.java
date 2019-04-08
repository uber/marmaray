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

package com.uber.marmaray.common.converters.data;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.uber.marmaray.common.AvroPayload;
import com.uber.marmaray.common.configuration.CassandraSinkConfiguration;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.converters.converterresult.ConverterResult;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.metrics.DataFeedMetricNames;
import com.uber.marmaray.common.metrics.DataFeedMetrics;
import com.uber.marmaray.common.metrics.ErrorCauseTagNames;
import com.uber.marmaray.common.metrics.ModuleTagNames;
import com.uber.marmaray.common.schema.cassandra.CassandraDataField;
import com.uber.marmaray.common.schema.cassandra.CassandraPayload;
import com.uber.marmaray.common.util.AbstractSparkTest;
import com.uber.marmaray.utilities.ByteBufferUtil;
import com.uber.marmaray.utilities.ErrorExtractor;
import com.uber.marmaray.utilities.SchemaUtil;
import com.uber.marmaray.utilities.TimestampInfo;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.cassandra.db.marshal.LongType;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static com.uber.marmaray.common.util.CassandraTestConstants.CONFIGURATION;

public class TestCassandraSinkDataConverter extends AbstractSparkTest {
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testNullInTestWriteAllFieldsMockDataToCassandraWithTimestampConverter() {
        final Schema schema = SchemaBuilder.builder().record("myRecord").fields()
            .name("longVal").type().nullable().longType().noDefault()
            .name("ts").type(SchemaUtil.getTimestampSchema(true)).noDefault()
            .name("longVal2").type().nullable().longType().noDefault()
            .name("floatVal").type().nullable().floatType().noDefault()
            .endRecord();
        final CassandraSinkDataConverter converter = new CassandraSinkDataConverter(
            schema, CONFIGURATION, Optional.absent(), Optional.absent(), Collections.emptyList(),
            TimestampInfo.generateEmptyTimestampInfo(), new ErrorExtractor());
        final GenericRecord gr = new GenericData.Record(schema);
        final long longVal2 = 123456L;
        final long ts = 1550944636123L;
        final float floatVal = 123456.789f;
        gr.put("longVal2", longVal2);
        gr.put("ts", ts);
        gr.put("floatVal", floatVal);
        final AvroPayload payload = new AvroPayload(gr);
        final List<ConverterResult<AvroPayload, CassandraPayload>> result = converter.convert(payload);
        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.get(0).getSuccessData().isPresent());
        final List<CassandraDataField> cassandraData = result.get(0).getSuccessData().get().getData().getData();
        Assert.assertEquals(4, cassandraData.size());
        Assert.assertEquals("longVal", ByteBufferUtil.convertToString(cassandraData.get(0).getColumnKey()));
        Assert.assertNull(cassandraData.get(0).getValue());
        Assert.assertEquals("ts", ByteBufferUtil.convertToString(cassandraData.get(1).getColumnKey()));
        Assert.assertEquals(ts, cassandraData.get(1).getValue().getLong());
        Assert.assertEquals("longVal2", ByteBufferUtil.convertToString(cassandraData.get(2).getColumnKey()));
        Assert.assertEquals(longVal2, cassandraData.get(2).getValue().getLong());
        Assert.assertEquals(floatVal, cassandraData.get(3).getValue().getFloat(), 0.00001);
    }

    @Test
    public void testNullTimestamp() {
        final Schema schema = SchemaBuilder.builder().record("myRecord").fields()
            .name("longVal").type().nullable().longType().noDefault()
            .name("ts").type(SchemaUtil.getTimestampSchema(true)).noDefault()
            .name("longVal2").type().nullable().longType().noDefault()
            .name("floatVal").type().nullable().floatType().noDefault()
            .endRecord();
        final CassandraSinkDataConverter converter = new CassandraSinkDataConverter(
            schema, CONFIGURATION, Optional.absent(), Optional.absent(), Collections.emptyList(),
            TimestampInfo.generateEmptyTimestampInfo(), new ErrorExtractor());
        final GenericRecord gr = new GenericData.Record(schema);
        final AvroPayload payload = new AvroPayload(gr);
        final List<ConverterResult<AvroPayload, CassandraPayload>> result = converter.convert(payload);
        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.get(0).getSuccessData().isPresent());
        final List<CassandraDataField> cassandraData = result.get(0).getSuccessData().get().getData().getData();
        Assert.assertEquals(4, cassandraData.size());
        Assert.assertEquals("longVal", ByteBufferUtil.convertToString(cassandraData.get(0).getColumnKey()));
        Assert.assertNull(cassandraData.get(0).getValue());
        Assert.assertEquals("ts", ByteBufferUtil.convertToString(cassandraData.get(1).getColumnKey()));
        Assert.assertNull(cassandraData.get(1).getValue());
        Assert.assertEquals("longVal2", ByteBufferUtil.convertToString(cassandraData.get(2).getColumnKey()));
        Assert.assertNull(cassandraData.get(2).getValue());
        Assert.assertEquals("floatVal", ByteBufferUtil.convertToString(cassandraData.get(3).getColumnKey()));
        Assert.assertNull(cassandraData.get(3).getValue());
    }

    @Test
    public void testSkipInvalidRow() {
        final Schema schema = SchemaBuilder.builder().record("myRecord").fields()
            .name("primary_key").type().nullable().stringType().noDefault().endRecord();
        final Configuration conf = getConfiguration();
        conf.setProperty(CassandraSinkConfiguration.SHOULD_SKIP_INVALID_ROWS, "true");

        final CassandraSinkDataConverter converter = new CassandraSinkDataConverter(
            schema, conf, Optional.absent(), Optional.absent(), ImmutableList.of("primary_key"),
            TimestampInfo.generateEmptyTimestampInfo(), new ErrorExtractor());

        // test null primary key
        final GenericRecord nullKey = new GenericData.Record(schema);
        nullKey.put("primary_key", null);
        final AvroPayload nullKeyPayload = new AvroPayload(nullKey);
        final List<ConverterResult<AvroPayload, CassandraPayload>> nullKeyResult = converter.convert(nullKeyPayload);
        Assert.assertEquals(1, nullKeyResult.size());
        Assert.assertTrue(nullKeyResult.get(0).getErrorData().isPresent());
        Assert.assertEquals("Required keys are missing. Keys: primary_key",
          nullKeyResult.get(0).getErrorData().get().getErrMessage());

        // test empty string primary key
        final GenericRecord emptyKey = new GenericData.Record(schema);
        emptyKey.put("primary_key", "");
        final AvroPayload emptyKeyPayload = new AvroPayload(emptyKey);
        final List<ConverterResult<AvroPayload, CassandraPayload>> emptyKeyResult = converter.convert(emptyKeyPayload);
        Assert.assertEquals(1, emptyKeyResult.size());
        Assert.assertTrue(emptyKeyResult.get(0).getErrorData().isPresent());
        Assert.assertEquals("Required keys are missing. Keys: primary_key",
          emptyKeyResult.get(0).getErrorData().get().getErrMessage());
    }

    @Test
    public void testByteArray() {
        final Configuration conf = getConfiguration();
        final Schema schema = SchemaBuilder.builder().record("myRecord").fields()
            .name("byteField").type().nullable().bytesType().noDefault()
            .endRecord();
        final GenericRecord record = new GenericData.Record(schema);
        final ByteBuffer value = ByteBuffer.wrap(new byte[]{0x4a, 0x64, 0x4c});
        record.put("byteField", value);
        final CassandraSinkDataConverter converter = new CassandraSinkDataConverter(
            schema, conf, Optional.absent(), Optional.absent(), Collections.emptyList(),
            TimestampInfo.generateEmptyTimestampInfo(), new ErrorExtractor());
        final List<ConverterResult<AvroPayload, CassandraPayload>> result = converter.convert(new AvroPayload(record));
        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.get(0).getSuccessData().isPresent());
        final List<CassandraDataField> cassandraData = result.get(0).getSuccessData().get().getData().getData();
        Assert.assertEquals(1, cassandraData.size());
        Assert.assertEquals(value, cassandraData.get(0).getValue());
    }

    @Test
    public void testLongTypeWrittenTime() {
        final Configuration conf = getConfiguration();

        final Schema schema = SchemaBuilder.builder().record("myRecord").fields()
                .name("timeField").type().nullable().longType().noDefault()
                .endRecord();
        final GenericRecord record = new GenericData.Record(schema);
        final Long value = 1934000000000L;
        record.put("timeField", value);
        final CassandraSinkDataConverter converter = new CassandraSinkDataConverter(
            schema, conf, Optional.absent(), Optional.of("timeField"), Collections.emptyList(),
            TimestampInfo.generateEmptyTimestampInfo(), new ErrorExtractor());
        final List<ConverterResult<AvroPayload, CassandraPayload>> result = converter.convert(new AvroPayload(record));
        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.get(0).getSuccessData().isPresent());
        final List<CassandraDataField> cassandraData = result.get(0).getSuccessData().get().getData().getData();
        Assert.assertEquals(2, cassandraData.size());
        Assert.assertEquals(value+"", LongType.instance.getString(cassandraData.get(0).getValue()));
        Assert.assertEquals(value+"000", LongType.instance.getString(cassandraData.get(1).getValue()));
    }

    @Test
    public void testStringTypeWrittenTime() {
        final Configuration conf = getConfiguration();

        final Schema schema = SchemaBuilder.builder().record("myRecord").fields()
                .name("timeField").type().nullable().stringType().noDefault()
                .endRecord();
        final GenericRecord record = new GenericData.Record(schema);
        final String value = "1934000000000";
        record.put("timeField", value);
        final CassandraSinkDataConverter converter = new CassandraSinkDataConverter(
            schema, conf, Optional.absent(), Optional.of("timeField"), Collections.emptyList(),
            TimestampInfo.generateEmptyTimestampInfo(), new ErrorExtractor());
        final List<ConverterResult<AvroPayload, CassandraPayload>> result = converter.convert(new AvroPayload(record));
        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.get(0).getSuccessData().isPresent());
        final List<CassandraDataField> cassandraData = result.get(0).getSuccessData().get().getData().getData();
        Assert.assertEquals(2, cassandraData.size());
    }

    @Test
    public void testNotSupportedTypeWrittenTime() {
        this.exception.expect(JobRuntimeException.class);
        this.exception.expectMessage("Order column type BYTES not supported. Only LONG and STRING type are supported.");

        final Configuration conf = getConfiguration();

        final Schema schema = SchemaBuilder.builder().record("myRecord").fields()
                .name("byteField").type().nullable().bytesType().noDefault()
                .endRecord();
        final GenericRecord record = new GenericData.Record(schema);
        final ByteBuffer value = ByteBuffer.wrap(new byte[]{0x4a, 0x64, 0x4c});
        record.put("byteField", value);
        final CassandraSinkDataConverter converter = new CassandraSinkDataConverter(
            schema, conf, Optional.absent(), Optional.of("byteField"), Collections.emptyList(),
            TimestampInfo.generateEmptyTimestampInfo(), new ErrorExtractor());
        converter.setDataFeedMetrics(new DataFeedMetrics("test_job", new HashMap<>()));
        converter.convert(new AvroPayload(record));
    }

    @Test
    public void testUnsupportedDataType() {
        this.exception.expect(JobRuntimeException.class);
        this.exception.expectMessage("Type ARRAY not supported");

        final Configuration conf = getConfiguration();
        final DataFeedMetrics mockMetrics = Mockito.mock(DataFeedMetrics.class);
        final Schema schema = SchemaBuilder.builder().record("myRecord").fields()
            .name("arrayField").type().array().items().nullable().stringType().noDefault()
            .endRecord();
        final GenericRecord record = new GenericData.Record(schema);
        record.put("arrayField", Collections.emptyList());
        final CassandraSinkDataConverter converter = new CassandraSinkDataConverter(
            schema, conf, Optional.absent(), Optional.absent(), Collections.emptyList(),
            TimestampInfo.generateEmptyTimestampInfo(), new ErrorExtractor());
        converter.setDataFeedMetrics(mockMetrics);
        try {
            converter.convert(new AvroPayload(record));
            Assert.fail();
        } catch (JobRuntimeException e) {
            Mockito.verify(mockMetrics).createLongFailureMetric(DataFeedMetricNames.MARMARAY_JOB_ERROR, 1,
                    DataFeedMetricNames.getErrorModuleCauseTags(
                            ModuleTagNames.SINK_CONVERTER, ErrorCauseTagNames.NOT_SUPPORTED_FIELD_TYPE));
            throw e;
        }
    }

    private Configuration getConfiguration() {
        final Configuration conf = new Configuration();
        conf.setProperty(CassandraSinkConfiguration.KEYSPACE, "keyspace");
        conf.setProperty(CassandraSinkConfiguration.TABLE_NAME, "table");
        conf.setProperty(CassandraSinkConfiguration.CLUSTER_NAME, "test-cluster");
        conf.setProperty(CassandraSinkConfiguration.PARTITION_KEYS, "primary_key");
        return conf;
    }
}
