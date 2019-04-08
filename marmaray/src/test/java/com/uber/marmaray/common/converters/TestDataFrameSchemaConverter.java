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

import com.uber.marmaray.common.converters.schema.DataFrameSchemaConverter;
import org.apache.avro.Schema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Test;

import static com.uber.marmaray.utilities.SchemaUtil.TIMESTAMP_PROPERTY;
import static com.uber.marmaray.utilities.SchemaUtil.TRUE;

public class TestDataFrameSchemaConverter {

    private static final String INT_TYPE = "intType";
    private static final String STRING_TYPE = "stringType";
    private static final String BOOLEAN_TYPE = "booleanType";
    private static final String DOUBLE_TYPE = "doubleType";
    private static final String BYTE_TYPE = "byteType";
    private static final String FLOAT_TYPE = "floatType";
    private static final String BINARY_TYPE = "binaryType";
    private static final String TIMESTAMP_TYPE = "timestampType";
    private static final String SHORT_TYPE = "shortType";
    private static final String LONG_TYPE = "longType";
    private static final String DATE_TYPE = "dateType";

    @Test
    public void testConvertSparkToAvroSchema() {
        // StructType is the type of schema returned from a DataFrame (aka Dataset<Row>)
        final StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField(INT_TYPE, DataTypes.IntegerType, false),
                DataTypes.createStructField(STRING_TYPE, DataTypes.StringType, false),
                DataTypes.createStructField(BOOLEAN_TYPE, DataTypes.BooleanType, false),
                DataTypes.createStructField(DOUBLE_TYPE, DataTypes.DoubleType, false),
                DataTypes.createStructField(BYTE_TYPE, DataTypes.ByteType, false),
                DataTypes.createStructField(FLOAT_TYPE, DataTypes.FloatType, false),
                DataTypes.createStructField(BINARY_TYPE, DataTypes.ByteType, false),
                DataTypes.createStructField(TIMESTAMP_TYPE, DataTypes.TimestampType, false),
                DataTypes.createStructField(SHORT_TYPE, DataTypes.IntegerType, false),
                DataTypes.createStructField(LONG_TYPE, DataTypes.LongType, false),
                DataTypes.createStructField(DATE_TYPE, DataTypes.DateType, false)
        });

        final DataFrameSchemaConverter converter = new DataFrameSchemaConverter();
        final Schema commonSchema = converter.convertToCommonSchema(schema);

        Assert.assertEquals(commonSchema.getType(), Schema.Type.RECORD);
        Assert.assertEquals(commonSchema.getFields().size(), 11);

        for (final Schema.Field field : commonSchema.getFields()) {
            switch (field.name()) {
                case INT_TYPE:
                    Assert.assertEquals(field.schema().getType(), Schema.Type.INT);
                    break;
                case STRING_TYPE:
                    Assert.assertEquals(field.schema().getType(), Schema.Type.STRING);
                    break;
                case BOOLEAN_TYPE:
                    Assert.assertEquals(field.schema().getType(), Schema.Type.BOOLEAN);
                    break;
                case DOUBLE_TYPE:
                    Assert.assertEquals(field.schema().getType(), Schema.Type.DOUBLE);
                    break;
                case BYTE_TYPE:
                    Assert.assertEquals(field.schema().getType(), Schema.Type.BYTES);
                    break;
                case FLOAT_TYPE:
                    Assert.assertEquals(field.schema().getType(), Schema.Type.FLOAT);
                    break;
                case BINARY_TYPE:
                    Assert.assertEquals(field.schema().getType(), Schema.Type.BYTES);
                    break;
                case TIMESTAMP_TYPE:
                    Assert.assertEquals(field.schema().getType(), Schema.Type.LONG);
                    Assert.assertEquals(field.schema().getProp(TIMESTAMP_PROPERTY), TRUE);
                    break;
                case SHORT_TYPE:
                    Assert.assertEquals(field.schema().getType(), Schema.Type.INT);
                    break;
                case LONG_TYPE:
                    Assert.assertEquals(field.schema().getType(), Schema.Type.LONG);
                    break;
                case DATE_TYPE:
                    Assert.assertEquals(field.schema().getType(), Schema.Type.STRING);
                    break;
            }
        }
    }
}
