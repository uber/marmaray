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
package com.uber.marmaray.common.sources.hive;

import com.google.common.collect.Sets;
import com.uber.marmaray.common.AvroPayload;
import com.uber.marmaray.common.configuration.HiveSourceConfiguration;
import com.uber.marmaray.common.converters.data.SparkSourceDataConverter;
import com.uber.marmaray.common.converters.schema.DataFrameSchemaConverter;
import com.uber.marmaray.common.metadata.HDFSMetadataManager;
import com.uber.marmaray.common.sources.IWorkUnitCalculator;
import com.uber.marmaray.common.util.AbstractSparkTest;
import com.uber.marmaray.common.util.FileHelperUtil;
import com.uber.marmaray.common.util.FileTestUtil;
import com.uber.marmaray.common.util.HiveTestUtil;
import com.uber.marmaray.utilities.ErrorExtractor;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestHiveSource extends AbstractSparkTest {

    private static final String LEFT_FIELD = "left";
    private static final String RIGHT_FIELD = "right";
    private static final String METADATA_KEY = "testData";

    private String metadataPath;

    @Before
    public void setupTest() {
        super.setupTest();
        this.metadataPath = FileTestUtil.getTempFolder();
    }

    @Test
    public void testReadDataFromParquetFile() throws IOException {
        final StructType dfSchema = DataTypes
            .createStructType(new StructField[]{
                DataTypes.createStructField(LEFT_FIELD, DataTypes.StringType, false),
                DataTypes.createStructField(RIGHT_FIELD, DataTypes.StringType, false)});
        final DataFrameSchemaConverter dfsc = new DataFrameSchemaConverter();
        final Schema avroSchema = dfsc.convertToCommonSchema(dfSchema);

        final String dataPath = FileHelperUtil.getResourcePath(getClass(), METADATA_KEY);
        final HiveSourceConfiguration hiveConf =
            HiveTestUtil.initializeConfig(JOB_NAME, dataPath);
        final SparkSourceDataConverter converter = new SparkSourceDataConverter(dfSchema, avroSchema,
            hiveConf.getConf(), Sets.newHashSet(LEFT_FIELD, RIGHT_FIELD), new ErrorExtractor());
        final HiveSource source = new HiveSource(hiveConf, this.sqlContext.get(), converter);

        final HDFSMetadataManager metadataManager = new HDFSMetadataManager(this.fileSystem.get(),
                new Path(this.metadataPath, METADATA_KEY).toString(),
                new AtomicBoolean(true));

        final ParquetWorkUnitCalculator calculator = new ParquetWorkUnitCalculator(hiveConf, this.fileSystem.get());
        calculator.initPreviousRunState(metadataManager);
        final IWorkUnitCalculator.IWorkUnitCalculatorResult<String, HiveRunState> results
                = calculator.computeWorkUnits();
        final JavaRDD<AvroPayload> rddData =
            source.getData((ParquetWorkUnitCalculatorResult) results);
        final List<AvroPayload> collectedData = rddData.collect();
        Assert.assertEquals(1, collectedData.size());

        final GenericRecord record = collectedData.get(0).getData();

        Assert.assertEquals("L", record.get(LEFT_FIELD).toString());
        Assert.assertEquals("R", record.get(RIGHT_FIELD).toString());
    }
}

