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

import com.datastax.driver.core.Statement;
import com.google.common.base.Optional;
import com.uber.marmaray.common.AvroPayload;
import com.uber.marmaray.common.configuration.CassandraSinkConfiguration;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.converters.converterresult.ConverterResult;
import com.uber.marmaray.common.util.AbstractSparkTest;
import com.uber.marmaray.utilities.ErrorExtractor;
import com.uber.marmaray.utilities.SchemaUtil;
import com.uber.marmaray.utilities.TimestampInfo;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;

import static com.uber.marmaray.common.configuration.CassandraSinkConfiguration.KEYSPACE;
import static com.uber.marmaray.common.configuration.CassandraSinkConfiguration.TABLE_NAME;

public class TestCassandraSinkCQLDataConverter extends AbstractSparkTest {

    private Configuration conf;

    @Before
    public void setup() {
        this.conf = new Configuration();
        this.conf.setProperty(TABLE_NAME, "myTable");
        this.conf.setProperty(KEYSPACE, "myKeyspace");

    }

    @Test
    public void testInsertNull() throws Exception {
        final Schema schema = SchemaBuilder.builder().record("myRecord").fields()
            .name("longVal").type().nullable().longType().noDefault()
            .endRecord();
        final CassandraSinkCQLDataConverter converter = new CassandraSinkCQLDataConverter(
            schema, this.conf, Optional.absent(), Collections.emptyList(),
            TimestampInfo.generateEmptyTimestampInfo(), new ErrorExtractor());
        final GenericRecord gr = new GenericData.Record(schema);
        final AvroPayload payload = new AvroPayload(gr);
        final List<ConverterResult<AvroPayload, Statement>> result =  converter.convert(payload);
        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.get(0).getSuccessData().isPresent());
        Assert.assertEquals("INSERT INTO myKeyspace.myTable () VALUES ();",
            result.get(0).getSuccessData().get().getData().toString());
    }

    @Test
    public void testInsertTimestamp() throws Exception {
        final Schema schema = SchemaBuilder.builder().record("myRecord").fields()
            .name("ts").type(SchemaUtil.getTimestampSchema(true)).noDefault()
            .name("longVal").type().nullable().longType().noDefault()
            .endRecord();
        final CassandraSinkCQLDataConverter converter = new CassandraSinkCQLDataConverter(
            schema, this.conf, Optional.absent(), Collections.emptyList(),
            TimestampInfo.generateEmptyTimestampInfo(), new ErrorExtractor());
        GenericRecord gr = new GenericData.Record(schema);
        gr.put("ts", SchemaUtil.encodeTimestamp(new Timestamp(1514844885123L)));
        gr.put("longVal", 123456L);
        AvroPayload payload = new AvroPayload(gr);
        List<ConverterResult<AvroPayload, Statement>> result =  converter.convert(payload);
        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.get(0).getSuccessData().isPresent());
        Assert.assertEquals("INSERT INTO myKeyspace.myTable (ts,longVal) VALUES (1514844885123,123456);",
            result.get(0).getSuccessData().get().getData().toString());
        // test null timestamp
        gr = new GenericData.Record(schema);
        gr.put("longVal", 123456L);
        gr.put("ts", null);
        payload = new AvroPayload(gr);
        result = converter.convert(payload);
        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.get(0).getSuccessData().isPresent());
        Assert.assertEquals("INSERT INTO myKeyspace.myTable (longVal) VALUES (123456);",
            result.get(0).getSuccessData().get().getData().toString());
    }

    @Test
    public void testInsertBinary() throws Exception {
        final Schema schema = SchemaBuilder.builder().record("myRecord").fields()
            .name("byteField").type().nullable().bytesType().noDefault()
            .endRecord();
        final CassandraSinkCQLDataConverter converter = new CassandraSinkCQLDataConverter(
            schema, this.conf, Optional.absent(), Collections.emptyList(),
            TimestampInfo.generateEmptyTimestampInfo(), new ErrorExtractor());
        final GenericRecord gr = new GenericData.Record(schema);
        gr.put("byteField", ByteBuffer.wrap(new byte[] {0x1c, 0x2f, 0x01, 0x34}));
        final AvroPayload payload = new AvroPayload(gr);
        final List<ConverterResult<AvroPayload, Statement>> result = converter.convert(payload);
        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.get(0).getSuccessData().isPresent());
        Assert.assertEquals("INSERT INTO myKeyspace.myTable (byteField) VALUES (0x1c2f0134);",
            result.get(0).getSuccessData().get().getData().toString());
    }

}
