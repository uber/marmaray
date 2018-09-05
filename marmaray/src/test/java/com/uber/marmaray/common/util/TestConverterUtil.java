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

package com.uber.marmaray.common.util;

import com.uber.marmaray.common.AvroPayload;
import com.uber.marmaray.utilities.ConverterUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;

public class TestConverterUtil extends AbstractSparkTest {

    @Test
    public void testConvertAvroPayloadToBytes() throws IOException {
        final Schema avroSchema = SchemaBuilder.record("commonSchema")
                .fields()
                .name("int").type().intType().noDefault()
                .name("boolean").type().booleanType().noDefault()
                .endRecord();

        final GenericRecord gr = new GenericData.Record(avroSchema);
        gr.put("int", Integer.MAX_VALUE);
        gr.put("boolean", true);

        final AvroPayload ap = new AvroPayload(gr);

        byte[] bytes = ConverterUtil.convertData(ap);

        final String json = new String(bytes);

        Assert.assertTrue(json.contains("int"));
        Assert.assertTrue(json.contains(String.valueOf(Integer.MAX_VALUE)));
        Assert.assertTrue(json.contains("boolean"));
        Assert.assertTrue(json.contains("true"));

        final JsonDecoder decoder = new DecoderFactory().jsonDecoder(avroSchema, new ByteArrayInputStream(bytes));
        final DatumReader<GenericRecord> reader = new GenericDatumReader<>(avroSchema);
        GenericRecord entry;
        while (true) {
            try {
                entry = reader.read(null, decoder);
                System.out.println(entry);
                Assert.assertTrue((Boolean) entry.get("boolean"));
                Assert.assertEquals(Integer.MAX_VALUE, entry.get("int"));
            } catch (EOFException exception) {
                break;
            }
        }

    }
}
