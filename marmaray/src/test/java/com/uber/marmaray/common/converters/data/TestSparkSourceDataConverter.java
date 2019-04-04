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

import com.uber.marmaray.common.AvroPayload;
import com.uber.marmaray.common.configuration.CassandraSinkConfiguration;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.converters.converterresult.ConverterResult;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.util.AbstractSparkTest;
import com.uber.marmaray.utilities.ErrorExtractor;
import com.uber.marmaray.utilities.SchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TestSparkSourceDataConverter extends AbstractSparkTest {

    public static final Schema OUTPUT_SCHEMA = SchemaBuilder.builder().record("commonSchema")
            .fields()
            .name("field1").type().intType().noDefault()
            .name("field2").type().longType().noDefault()
            .name("field3").type(SchemaUtil.getTimestampSchema(true)).noDefault()
            .name("field4").type().nullable().bytesType().noDefault()
            .endRecord();
    public static final StructType INPUT_SCHEMA = new StructType(new StructField[]{
            new StructField("field1", DataTypes.IntegerType, false, null),
            new StructField("field2", DataTypes.LongType, false, null),
            new StructField("field3", DataTypes.TimestampType, true, null),
            new StructField("field4", DataTypes.BinaryType, true, null)});

    @Test
    public void convert() throws Exception {
        final byte[] binaryData = new byte[] {0x12, 0x34, 0x56};
        final Row data = new GenericRowWithSchema(new Object[] {1, 2L, new Timestamp(123456789), binaryData}, INPUT_SCHEMA);
        final SparkSourceDataConverter converter = new SparkSourceDataConverter(INPUT_SCHEMA, OUTPUT_SCHEMA, new Configuration(), Collections.emptySet(), new ErrorExtractor());
        final List<ConverterResult<Row, AvroPayload>> results = converter.convert(data);
        Assert.assertEquals(1, results.size());
        Assert.assertTrue(results.get(0).getSuccessData().isPresent());
        final AvroPayload resultPayload = results.get(0).getSuccessData().get().getData();
        Assert.assertEquals(ByteBuffer.wrap(binaryData), resultPayload.getData().get("field4"));
    }

    @Test
    public void testNulls() throws Exception {
        final Row data = new GenericRowWithSchema(new Object[] {1, 2L, null, null}, INPUT_SCHEMA);
        final SparkSourceDataConverter converter = new SparkSourceDataConverter(INPUT_SCHEMA, OUTPUT_SCHEMA, new Configuration(), Collections.emptySet(), new ErrorExtractor());
        final List<ConverterResult<Row, AvroPayload>> results = converter.convert(data);
        Assert.assertEquals(1, results.size());
        Assert.assertTrue(results.get(0).getSuccessData().isPresent());
        final AvroPayload resultPayload = results.get(0).getSuccessData().get().getData();
        Assert.assertEquals(1, resultPayload.getData().get("field1"));
        Assert.assertEquals(2L, resultPayload.getData().get("field2"));
        Assert.assertNull(resultPayload.getData().get("field3"));
        Assert.assertNull(resultPayload.getData().get("field4"));
    }

    @Test
    public void testRequiredFields() throws Exception {
        final Row data = new GenericRowWithSchema(new Object[] {1, 2L, null, null}, INPUT_SCHEMA);
        final Set<String> requiredFields = new HashSet<>(Arrays.asList("field1", "field4"));
        final Configuration conf = new Configuration();
        final SparkSourceDataConverter converter = new SparkSourceDataConverter(INPUT_SCHEMA, OUTPUT_SCHEMA, conf, requiredFields, new ErrorExtractor());
        try {
            converter.convert(data);
            Assert.fail("conversion should fail due to missing key");
        } catch (JobRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("Required fields were missing."));
        }
        conf.setProperty(CassandraSinkConfiguration.SHOULD_SKIP_INVALID_ROWS, "true");
        final List<ConverterResult<Row, AvroPayload>> results = converter.convert(data);
        Assert.assertEquals(1, results.size());
        Assert.assertFalse(results.get(0).getSuccessData().isPresent());
        Assert.assertTrue(results.get(0).getErrorData().isPresent());
    }

}
