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
package com.uber.marmaray.common.converters;

import com.google.common.base.Optional;
import com.uber.marmaray.common.AvroPayload;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.converters.data.CassandraSinkDataConverter;
import com.uber.marmaray.common.schema.cassandra.CassandraDataField;
import com.uber.marmaray.common.schema.cassandra.CassandraPayload;
import com.uber.marmaray.common.util.AbstractSparkTest;
import com.uber.marmaray.utilities.ByteBufferUtil;
import com.uber.marmaray.utilities.ErrorExtractor;
import com.uber.marmaray.utilities.TimestampInfo;
import java.util.Collections;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Test;

public class TestCassandraDataFrameConverter extends AbstractSparkTest {

    private static final String INT_FIELD = "int_field";
    private static final String DOUBLE_FIELD = "double_field";
    private static final String STRING_FIELD = "string_field";
    private static final String BOOLEAN_FIELD = "boolean_field";

    @Test
    public void convertCommonSchemaDataToCassandra() {
        final Schema avroSchema = SchemaBuilder.record("commonSchema")
                .fields()
                .name(INT_FIELD).type().intType().noDefault()
                .name(DOUBLE_FIELD).type().doubleType().noDefault()
                .name(STRING_FIELD).type().stringType().noDefault()
                .name(BOOLEAN_FIELD).type().booleanType().noDefault()
                .endRecord();

        final GenericRecord gr = new GenericData.Record(avroSchema);
        gr.put(INT_FIELD, Integer.MAX_VALUE);
        gr.put(DOUBLE_FIELD, Double.MAX_VALUE);
        gr.put(STRING_FIELD, "foo");
        gr.put(BOOLEAN_FIELD, true);

        final AvroPayload ap = new AvroPayload(gr);
        final List<AvroPayload> records = Collections.singletonList(ap);
        final JavaRDD<AvroPayload> payloadRDD = this.jsc.get().parallelize(records);

        final CassandraSinkDataConverter csdc = new CassandraSinkDataConverter(avroSchema,
                new Configuration(),
                Optional.absent(),
                Collections.EMPTY_LIST,
                TimestampInfo.generateEmptyTimestampInfo(), new ErrorExtractor());

        final JavaRDD<CassandraPayload> cassRDD = csdc.map(payloadRDD).getData();
        final List<CassandraPayload> payloads = cassRDD.collect();

        Assert.assertEquals(payloads.size(), 1);
        final CassandraPayload cp = payloads.get(0);
        final List<CassandraDataField> fields = cp.getData();

        Assert.assertEquals(4, fields.size());

        for (int i = 0; i <= 3; i++) {
            final CassandraDataField field = fields.get(i);

            switch (i) {
                case 0:
                    Assert.assertEquals(INT_FIELD, ByteBufferUtil.convertToString(field.getColumnKey()));
                    Assert.assertEquals(Integer.MAX_VALUE, field.getValue().getInt());
                    break;

                case 1:
                    Assert.assertEquals(ByteBufferUtil.convertToString(field.getColumnKey()), DOUBLE_FIELD);
                    Assert.assertEquals(Double.MAX_VALUE, field.getValue().getDouble(), 0.0);
                    break;

                case 2:
                    Assert.assertEquals(ByteBufferUtil.convertToString(field.getColumnKey()), STRING_FIELD);
                    Assert.assertEquals("foo", ByteBufferUtil.convertToString(field.getValue()));
                    break;

                case 3:
                    Assert.assertEquals(BOOLEAN_FIELD, ByteBufferUtil.convertToString(field.getColumnKey()));
                    boolean value = field.getValue().get() > 0 ? true : false;
                    Assert.assertTrue(value);
                    break;
            }
        }
    }
}
