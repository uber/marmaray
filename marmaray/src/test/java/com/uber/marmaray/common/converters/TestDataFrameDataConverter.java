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
import com.uber.marmaray.common.converters.data.SparkSourceDataConverter;
import com.uber.marmaray.common.converters.schema.DataFrameSchemaConverter;
import com.uber.marmaray.common.util.SparkTestUtil;
import com.uber.marmaray.utilities.ErrorExtractor;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Tests for converting data from DataFrame (Spark) -> Avro schema with data
 */
public class TestDataFrameDataConverter {

    private static final String STRING_FIELD = "string_field";
    private static final String INT_FIELD = "int_field";
    private static final String BOOL_FIELD = "bool_field";
    private static final String DOUBLE_FIELD = "double_field";

    private Optional<SparkSession> spark = Optional.absent();
    private Optional<JavaSparkContext> jsc = Optional.absent();

    @Before
    public void setupTest() {
        final SparkConf sparkConf = SparkTestUtil.getSparkConf(TestCassandraDataFrameConverter.class.getName());
        this.spark = Optional.of(SparkTestUtil.getSparkSession(sparkConf));
        this.jsc = Optional.of(new JavaSparkContext(spark.get().sparkContext()));
    }

    @After
    public void teardownTest() {
        if (this.spark.isPresent()) {
            this.spark.get().stop();
            this.spark = Optional.absent();
        }

        if (this.jsc.isPresent()) {
            this.jsc.get().stop();
            this.jsc = Optional.absent();
        }
    }

    @Test
    public void convertDataFrameToCommonDataSchemaTest() {
        final List<Object[]> dataList = new ArrayList<>();
        dataList.add(new Object[] {"data0", Integer.MIN_VALUE, true, Double.MIN_VALUE});
        dataList.add(new Object[] {"data1", Integer.MAX_VALUE, false, Double.MAX_VALUE});

        final JavaRDD<Row> rowRDDNoSchema = this.jsc.get()
                .parallelize(dataList).map((Object[] row) -> RowFactory.create(row));

        final StructType schema = DataTypes
                .createStructType(new StructField[] {
                        DataTypes.createStructField(STRING_FIELD, DataTypes.StringType, false),
                        DataTypes.createStructField(INT_FIELD, DataTypes.IntegerType, false),
                        DataTypes.createStructField(BOOL_FIELD, DataTypes.BooleanType, false),
                        DataTypes.createStructField(DOUBLE_FIELD, DataTypes.DoubleType, false)});

        final Dataset<Row> df = this.spark.get().createDataFrame(rowRDDNoSchema, schema);
        final DataFrameSchemaConverter dfsc = new DataFrameSchemaConverter();
        final Schema commonSchema = dfsc.convertToCommonSchema(df.schema());

        final SparkSourceDataConverter converter =
            new SparkSourceDataConverter(schema, commonSchema, new Configuration(), Collections.singleton(STRING_FIELD),
                                            new ErrorExtractor());
        final JavaRDD<AvroPayload> payloadRDD = converter.map(df.javaRDD()).getData();

        final List<AvroPayload> payloads = payloadRDD.collect();

        Assert.assertEquals(2, payloads.size());

        final AvroPayload ap = payloads.get(0);
        final GenericRecord gr = ap.getData();

        Assert.assertEquals("data0", gr.get(STRING_FIELD).toString());
        Assert.assertEquals(Integer.MIN_VALUE, gr.get(INT_FIELD));
        Assert.assertEquals(true, gr.get(BOOL_FIELD));
        Assert.assertEquals(Double.MIN_VALUE, gr.get(DOUBLE_FIELD));

        final AvroPayload ap2 = payloads.get(1);
        final GenericRecord gr2 = ap2.getData();

        Assert.assertEquals("data1", gr2.get(STRING_FIELD).toString());
        Assert.assertEquals(Integer.MAX_VALUE, gr2.get(INT_FIELD));
        Assert.assertEquals(false, gr2.get(BOOL_FIELD));
        Assert.assertEquals(Double.MAX_VALUE, gr2.get(DOUBLE_FIELD));
    }

    @Test
    public void convertDataFrameToCommonDataNotAllColumnsPopulatedTest() throws Exception {
        final List<Object[]> dataList = new ArrayList<>();
        dataList.add(new Object[] {"data0", Integer.MIN_VALUE, false});

        final JavaRDD<Row> rowRDDNoSchema = this.jsc.get()
                .parallelize(dataList).map((Object[] row) -> RowFactory.create(row));

        final StructType schemaWithMissingField = DataTypes
                .createStructType(new StructField[] {
                        DataTypes.createStructField(STRING_FIELD, DataTypes.StringType, false),
                        DataTypes.createStructField(INT_FIELD, DataTypes.IntegerType, false),
                        DataTypes.createStructField(BOOL_FIELD, DataTypes.BooleanType, false)});

        final StructType expectedSchema = DataTypes
                .createStructType(new StructField[] {
                        DataTypes.createStructField(STRING_FIELD, DataTypes.StringType, false),
                        DataTypes.createStructField(INT_FIELD, DataTypes.IntegerType, false),
                        DataTypes.createStructField(BOOL_FIELD, DataTypes.BooleanType, false),
                        DataTypes.createStructField(DOUBLE_FIELD, DataTypes.DoubleType, true)});

        final Dataset<Row> df = this.spark.get().createDataFrame(rowRDDNoSchema, schemaWithMissingField);
        final DataFrameSchemaConverter dfsc = new DataFrameSchemaConverter();
        final Schema commonSchema = dfsc.convertToCommonSchema(expectedSchema);

        final List<Row> rows = df.javaRDD().collect();

        final SparkSourceDataConverter converter =
                new SparkSourceDataConverter(expectedSchema, commonSchema,
                        new Configuration(), Collections.singleton(STRING_FIELD), new ErrorExtractor());

        // The missing field should be handled gracefully without failing since rows can have sparse data
        final AvroPayload payload = converter.convert(rows.get(0)).get(0).getSuccessData().get().getData();
        final GenericRecord gr = payload.getData();

        Assert.assertEquals("data0", gr.get(STRING_FIELD).toString());
        Assert.assertEquals(Integer.MIN_VALUE, gr.get(INT_FIELD));
        Assert.assertEquals(false, gr.get(BOOL_FIELD));
        Assert.assertNull(gr.get(DOUBLE_FIELD));
    }
}
