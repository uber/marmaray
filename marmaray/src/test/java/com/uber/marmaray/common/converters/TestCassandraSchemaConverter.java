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
import com.uber.marmaray.common.converters.schema.CassandraSchemaConverter;
import com.uber.marmaray.common.schema.cassandra.CassandraSchema;
import com.uber.marmaray.common.schema.cassandra.CassandraSchemaField;
import com.uber.marmaray.common.util.SchemaTestUtil;
import com.uber.marmaray.utilities.SchemaUtil;
import com.uber.marmaray.utilities.TimestampInfo;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;

import static com.uber.marmaray.common.schema.cassandra.CassandraSchemaField.TIMESTAMP_TYPE;

public class TestCassandraSchemaConverter {
    @Test
    public void testConvertCommonToCassandraSchemaNoTimestamp() {
        final Schema record = SchemaBuilder.record("commonSchema")
                .fields()
                .name("field0").type().intType().noDefault()
                .name("field1").type().doubleType().noDefault()
                .name("field2").type().stringType().noDefault()
                .name("field3").type().booleanType().noDefault()
                .endRecord();

        final CassandraSchemaConverter converter = new CassandraSchemaConverter("testKeyspace",
                "testTableName", Optional.absent());

        Assert.assertEquals("testKeyspace", converter.getKeySpace());
        Assert.assertEquals("testTableName", converter.getTableName());

        final CassandraSchema cassSchema = converter.convertToExternalSchema(record);

        Assert.assertTrue(cassSchema.getFields().size() == 4);

        for (int i = 0; i <= 3; i++) {
            final CassandraSchemaField field = cassSchema.getFields().get(i);
            Assert.assertEquals(field.getFieldName(), "field" + i);

            switch (i) {
                case 0:
                    Assert.assertEquals(CassandraSchemaField.convertFromAvroType(
                            SchemaTestUtil.getSchema(CassandraSchemaField.INT_TYPE)),
                            field.getType());
                    break;
                case 1:
                    Assert.assertEquals(CassandraSchemaField.convertFromAvroType(
                            SchemaTestUtil.getSchema(CassandraSchemaField.DOUBLE_TYPE)),
                            field.getType());
                    break;
                case 2:
                    Assert.assertEquals(CassandraSchemaField.convertFromAvroType(
                            SchemaTestUtil.getSchema(CassandraSchemaField.STRING_TYPE)),
                            field.getType());
                    break;
                case 3:
                    Assert.assertEquals(CassandraSchemaField.convertFromAvroType(
                            SchemaTestUtil.getSchema(CassandraSchemaField.BOOLEAN_TYPE)),
                            field.getType());
                    break;
                default:
                    throw new RuntimeException("Field was unexpected");
            }
        }
    }

    @Test
    public void testConvertCommonToCassandraSchemaWithStringTimestamp() {
        final Schema record = SchemaBuilder.record("commonSchema")
                .fields()
                .name("field0").type().intType().noDefault()
                .endRecord();

        final TimestampInfo tsInfo = new TimestampInfo(Optional.of("10000"), false, "testTimestamp");
        final CassandraSchemaConverter converter = new CassandraSchemaConverter("testKeyspace",
                "testTableName", tsInfo, Optional.absent());
        final CassandraSchema cassSchema = converter.convertToExternalSchema(record);

        Assert.assertTrue(cassSchema.getFields().size() == 2);

        final CassandraSchemaField intField = cassSchema.getFields().get(0);
        Assert.assertEquals("field0", intField.getFieldName());
        Assert.assertEquals(CassandraSchemaField.convertFromAvroType(
                SchemaTestUtil.getSchema(CassandraSchemaField.INT_TYPE)), intField.getType());

        final CassandraSchemaField timestampField = cassSchema.getFields().get(1);
        Assert.assertEquals("testTimestamp", timestampField.getFieldName());
        Assert.assertEquals(CassandraSchemaField.convertFromAvroType(
                SchemaTestUtil.getSchema(CassandraSchemaField.STRING_TYPE)),
                timestampField.getType());
    }

    @Test
    public void testConvertCommonToCassandraSchemaWithTimestampType() {
        final Schema record = SchemaBuilder.record("commonSchema")
            .fields()
            .name("ts").type(SchemaUtil.getTimestampSchema(true)).noDefault()
            .endRecord();
        final CassandraSchemaConverter converter = new CassandraSchemaConverter("testKeyspace",
            "testTableName", Optional.absent());
        final CassandraSchema cassSchema = converter.convertToExternalSchema(record);
        Assert.assertEquals(TIMESTAMP_TYPE, cassSchema.getFields().get(0).getType());
    }

    @Test
    public void testConvertCommonToCassandraSchemaRemovesFieldStartingWithUnderscore() {
        final Schema record = SchemaBuilder.record("commonSchema")
                .fields()
                .name("_field0").type().intType().noDefault()
                .name("_field1").type().doubleType().noDefault()
                .name("_field2").type().stringType().noDefault()
                .name("field3").type().booleanType().noDefault()
                .endRecord();

        final CassandraSchemaConverter converter = new CassandraSchemaConverter("testKeyspace",
                "testTableName", Optional.absent());
        final CassandraSchema cassSchema = converter.convertToExternalSchema(record);
        Assert.assertTrue(cassSchema.getFields().size() == 1);
        Assert.assertEquals("field3", cassSchema.getFields().get(0).getFieldName());
        Assert.assertEquals(CassandraSchemaField.convertFromAvroType(
                SchemaTestUtil.getSchema(CassandraSchemaField.BOOLEAN_TYPE)),
                cassSchema.getFields().get(0).getType());

    }

    @Test
    public void testConvertCommonToCassandraSchemaWithFilteredFields() {
        final Schema record = SchemaBuilder.record("commonSchema")
                .fields()
                .name("field0").type().intType().noDefault()
                .name("field1").type().doubleType().noDefault()
                .name("field2").type().stringType().noDefault()
                .name("field3").type().booleanType().noDefault()
                .endRecord();

        final CassandraSchemaConverter converter = new CassandraSchemaConverter("testKeyspace",
                "testTableName", Optional.of(new HashSet<>(Arrays.asList("field2", "field3"))));
        final CassandraSchema cassSchema = converter.convertToExternalSchema(record);
        Assert.assertTrue(cassSchema.getFields().size() == 2);
        Assert.assertEquals("field2", cassSchema.getFields().get(0).getFieldName());
        Assert.assertEquals("field3", cassSchema.getFields().get(1).getFieldName());
        Assert.assertEquals(CassandraSchemaField.convertFromAvroType(
                SchemaTestUtil.getSchema(CassandraSchemaField.STRING_TYPE)),
                cassSchema.getFields().get(0).getType());
        Assert.assertEquals(CassandraSchemaField.convertFromAvroType(
                SchemaTestUtil.getSchema(CassandraSchemaField.BOOLEAN_TYPE)),
                cassSchema.getFields().get(1).getType());
    }
}
