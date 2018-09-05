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
import com.uber.marmaray.common.converters.data.FileSinkDataConverter;
import com.uber.marmaray.common.util.AvroPayloadUtil;
import com.uber.marmaray.utilities.ErrorExtractor;
import com.uber.marmaray.utilities.StringTypes;
import lombok.NonNull;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import lombok.extern.slf4j.Slf4j;
import org.mockito.Matchers;
import static org.junit.Assert.*;

@Slf4j
public class TestHdfsFileSink  extends FileSinkTestUtil {
    private static final String PATH1 = "/testpath/test1";
    private static final String PATH2 = "/testpath/test2";
    private static final String PATH3 = "/testpath/test3";
    private static final String PATH4 = "/testpath/test4";
    private static final String PARENT_PATH = "testpath";
    private static final String COMMA_SEPARATOR = ",";
    private static final String SPECIAL_SEPARATOR = "\021";
    private static final int NUM_RECORD1 = 100;
    private static final int NUM_RECORD2 = 200;
    private static final int EXPECTED_PARTITION_NUM1 = 1;
    private static final int EXPECTED_PARTITION_NUM2 = 2;
    private static final String TIMESTAMP1 = "201808011025";
    private static final String TIMESTAMP2 = "201808012025";
    private static final String TIMESTAMP3 = "201808022025";
    private static final String SOURCE_SUB_PATH1 = "2018/08/01";
    private static final String SOURCE_SUB_PATH2 = "2018/08/02";
    private static final String VERSION = "version";
    private static final String OVERWRITE = "overwrite";
    @Before
    public void setupTest() {
        super.setupTest();
    }

    @After
    public void tearDownTest() throws IOException {
        this.fileSystem.get().delete(new Path(PARENT_PATH), true);
        super.teardownTest();
    }

    @Test
    public void testWriteToCsvWithCommaSeparatorWithVersion() throws Exception {
        final JavaRDD<AvroPayload> testData = AvroPayloadUtil.generateTestData(this.jsc.get(),
                NUM_RECORD1,
                StringTypes.EMPTY);
        testWriteToCsvCommon(this.pathPrefix, PATH1, COMMA_SEPARATOR, testData, EXPECTED_PARTITION_NUM1, TIMESTAMP1, SOURCE_SUB_PATH1, VERSION);
        testWriteToCsvCommon(this.pathPrefix, PATH1, COMMA_SEPARATOR, testData, EXPECTED_PARTITION_NUM2, TIMESTAMP2, SOURCE_SUB_PATH1, VERSION);
        testWriteToCsvCommon(this.pathPrefix, PATH1, COMMA_SEPARATOR, testData, EXPECTED_PARTITION_NUM1, TIMESTAMP3, SOURCE_SUB_PATH2, VERSION);
    }

    @Test
    public void testWriteToCsvWithSpecialCharAndSeparatorWithOverwrite() throws Exception {
        final JavaRDD<AvroPayload> testData = AvroPayloadUtil.generateTestDataNew(this.jsc.get(),
                NUM_RECORD1,
                StringTypes.EMPTY);
        testWriteToCsvCommon(this.pathPrefix, PATH2, SPECIAL_SEPARATOR, testData, EXPECTED_PARTITION_NUM2, TIMESTAMP1, SOURCE_SUB_PATH1, VERSION);
        testWriteToCsvCommon(this.pathPrefix, PATH2, COMMA_SEPARATOR, testData, EXPECTED_PARTITION_NUM2, TIMESTAMP3, SOURCE_SUB_PATH2, VERSION);
        testWriteToCsvCommon(this.pathPrefix, PATH2, COMMA_SEPARATOR, testData, EXPECTED_PARTITION_NUM2, TIMESTAMP2, SOURCE_SUB_PATH1, OVERWRITE);
    }

    @Test
    public void testWriteToCsvWithOverwriteAtStart() throws Exception {
        final JavaRDD<AvroPayload> testData = AvroPayloadUtil.generateTestData(this.jsc.get(),
                NUM_RECORD1,
                StringTypes.EMPTY);
        testWriteToCsvCommon(this.pathPrefix, PATH3, SPECIAL_SEPARATOR, testData, EXPECTED_PARTITION_NUM1, TIMESTAMP1, SOURCE_SUB_PATH1, OVERWRITE);
    }

    @Test
    public void testWriteToCsvWithHeader() throws IOException {
        final JavaRDD<AvroPayload> testData = AvroPayloadUtil.generateTestDataNew(this.jsc.get(),
                NUM_RECORD2,
                StringTypes.EMPTY);
        final Configuration conf = initConfig(pathPrefix, PATH4, COMMA_SEPARATOR, TIMESTAMP1, SOURCE_SUB_PATH1, VERSION);
        conf.setProperty(FileSinkConfiguration.CSV_COLUMN_HEADER, "true");
        final FileSinkDataConverter converter = new FileSinkDataConverter(conf, new ErrorExtractor());
        final FileSinkConfiguration fileConf = new FileSinkConfiguration(conf);
        final HdfsFileSink hdfsSink = spy(new HdfsFileSink(fileConf, converter));
        hdfsSink.write(testData);
        verify(hdfsSink, times(1)).write(Matchers.any(JavaRDD.class));
        verify(hdfsSink, times(1)).addColumnHeader(Matchers.anyString(), Matchers.any(JavaRDD.class));
        final FileStatus[] status = this.fileSystem.get().listStatus(new Path(fileConf.getPathHdfs()));
        for (final FileStatus fileStatus : status) {
            if (fileStatus.isFile()) {
                Path path = fileStatus.getPath();
                FSDataInputStream in = this.fileSystem.get().open(path);
                BufferedReader d = new BufferedReader(new InputStreamReader(in));
                String header = d.readLine();
                Assert.assertEquals("int_field,string_field,boolean_field", header);
                in.close();
                d.close();
            }
        }
    }

    private void testWriteToCsvCommon(@NonNull final String pathPrefix, @NonNull final String path,
                                      @NonNull final String separator, @NonNull final JavaRDD<AvroPayload> testData,
                                      @NonNull final int partitionNum, @NonNull final String timeStamp,
                                      @NonNull final String sourceSubPath, @NonNull final String dispersalType) throws Exception {
        final Configuration conf = initConfig(pathPrefix, path, separator, timeStamp, sourceSubPath, dispersalType);
        final FileSinkDataConverter converter = new FileSinkDataConverter(conf, new ErrorExtractor());
        final FileSinkConfiguration fileConf = new FileSinkConfiguration(conf);
        final HdfsFileSink hdfsSink = spy(new HdfsFileSink(fileConf, converter));
        hdfsSink.write(testData);
        verify(hdfsSink, times(1)).write(Matchers.any(JavaRDD.class));
        verify(hdfsSink, times(1)).getRepartitionNum(Matchers.any(JavaRDD.class));
        verify(hdfsSink, times(1)).getRddSizeInMegaByte(Matchers.any(JavaRDD.class));
        final FileStatus[] status = this.fileSystem.get().listStatus(new Path(fileConf.getPathHdfs()));
        int fileNum = 0;
        for (final FileStatus fileStatus : status) {
            if (fileStatus.isFile()) {
                fileNum++;
            }
        }
        assertEquals(partitionNum, fileNum);
    }
}
