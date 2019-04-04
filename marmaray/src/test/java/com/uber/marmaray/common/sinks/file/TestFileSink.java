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

package com.uber.marmaray.common.sinks.file;

import com.uber.marmaray.common.AvroPayload;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.configuration.FileSinkConfiguration;
import com.uber.marmaray.common.converters.data.FileSinkDataCSVConverter;
import com.uber.marmaray.common.util.AvroPayloadUtil;
import com.uber.marmaray.utilities.ErrorExtractor;
import com.uber.marmaray.utilities.StringTypes;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import static org.mockito.Mockito.*;

import java.io.IOException;

import static org.junit.Assert.*;

@Slf4j
public class TestFileSink extends FileSinkTestUtil{
    private static final String PARENT_PATH = "/testpath";
    private static final String PATH1 = "/testpath/test1";
    private static final int NUM_RECORD1 = 100;
    private static final int NUM_RECORD2 = 2000;
    private static final String COMMA_SEPARATOR = ",";
    private static final int EXPECTED_PARTITION_NUM = 1;
    private static final long EXPECTED_SAMPLE_SIZE = 1084;
    private static final String TIMESTAMP1 = "201808022025";
    private static final String SOURCE_SUB_PATH1 = "2018/08/01";
    private static final String VERSION = "version";
    private JavaRDD<String> convertedData1;
    private JavaRDD<AvroPayload> testData1;
    private JavaRDD<String> convertedData2;
    private JavaRDD<AvroPayload> testData2;
    private Configuration conf;
    private FileSinkDataCSVConverter converter;
    private FileSink fileSink;

    @Before
    public void setupTest() {
        super.setupTest();
        this.testData1 = AvroPayloadUtil.generateTestData(this.jsc.get(),
                NUM_RECORD1,
                StringTypes.EMPTY);
        this.testData2 = AvroPayloadUtil.generateTestDataNew(this.jsc.get(),
                NUM_RECORD2,
                StringTypes.EMPTY);
        this.conf = initConfig(pathPrefix, PATH1, COMMA_SEPARATOR, TIMESTAMP1, SOURCE_SUB_PATH1, VERSION);
        this.converter = new FileSinkDataCSVConverter(conf, new ErrorExtractor());
        final FileSinkConfiguration fileConf = new FileSinkConfiguration(conf);
        this.fileSink = spy(new HdfsFileSink(fileConf, converter));

        final JavaPairRDD<String, String> tmpData1 = this.converter.convertAll(this.testData1);
        this.convertedData1 = tmpData1.map(message -> message._2());
        final JavaPairRDD<String, String> tmpData2 = this.converter.convertAll(this.testData2);
        this.convertedData2 = tmpData2.map(message -> message._2());
    }

    @After
    public void tearDownTest() throws IOException {
        this.fileSystem.get().delete(new Path(PARENT_PATH), true);
        super.teardownTest();
    }

    @Test
    public void testGetRepartitionNum() {
        final int partitionNum = this.fileSink.getRepartitionNum(this.convertedData1);
        assertEquals(EXPECTED_PARTITION_NUM, partitionNum);
    }

    @Test
    public void testGetRddSizeNoMoreThanSampleRow() {
            final double rddSize = fileSink.getRddSizeInMegaByte(this.convertedData1);
            final long sampleSize = fileSink.getSampleSizeInBytes(this.convertedData1);
            final double sampleSizeInMB = (double) sampleSize / FileUtils.ONE_MB;
            assertEquals(rddSize, sampleSizeInMB, 0.1);
    }

    @Test
    public void testGetRddSizeMoreThanSampleRow() {
        final double rddSize = fileSink.getRddSizeInMegaByte(this.convertedData2);
        verify(this.fileSink, times(1)).getSampleSizeInBytes(Matchers.any(JavaRDD.class));
        final long sampleSize = fileSink.getSampleSizeInBytes(convertedData2);
        final double sampleSizeInMB = (double) sampleSize / FileUtils.ONE_MB;
        /*File size is sampled result, so not accurate"*/
        log.info("Exact file size[MB]: {}", sampleSizeInMB);
        log.info("Sample file size[MB]: {}", rddSize);
    }

    @Test
    public void testGetSampleSizeInBytes() {
        final long sampleSize = fileSink.getSampleSizeInBytes(this.convertedData1);
        assertEquals(EXPECTED_SAMPLE_SIZE, sampleSize);
    }

}
