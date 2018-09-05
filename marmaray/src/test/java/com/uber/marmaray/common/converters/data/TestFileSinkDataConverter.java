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
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.configuration.FileSinkConfiguration;
import com.uber.marmaray.common.util.AbstractSparkTest;
import com.uber.marmaray.common.util.AvroPayloadUtil;
import com.uber.marmaray.utilities.ErrorExtractor;
import com.uber.marmaray.utilities.StringTypes;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Test;

@Slf4j
public class TestFileSinkDataConverter extends AbstractSparkTest {
    @Test
    public void testConvertAllWithCsv() {
        log.info("Starts Test convert all with csv");
        final String separator = " ";
        final JavaRDD<AvroPayload> payloadData = AvroPayloadUtil.generateTestData(this.jsc.get(), 10, StringTypes.EMPTY);
        final Configuration conf = initConf(separator, "csv");
        log.info("Starting to convert data.");
        final FileSinkDataConverter converter = new FileSinkDataConverter(conf, new ErrorExtractor());
        final JavaRDD<String> dataConverted = converter.convertAll(payloadData);
        int i = 1;
        for (String line: dataConverted.collect()){
            Assert.assertEquals(String.valueOf(i) + separator + String.valueOf(i)  + separator + "true", line );
            i = i + 1;
        }
    }

    @Test
    public void testConvertAllWithCsvSpecialChar() {
        log.info("Starts Test convert all with csv");
        final String separator = ",";
        final JavaRDD<AvroPayload> payloadData = AvroPayloadUtil.generateTestDataNew(this.jsc.get(), 10, StringTypes.EMPTY);
        final Configuration conf = initConf(separator, "csv");
        log.info("Starting to convert data.");
        final FileSinkDataConverter converter = new FileSinkDataConverter(conf, new ErrorExtractor());
        final JavaRDD<String> dataConverted = converter.convertAll(payloadData);
        int i = 1;
        for (String line: dataConverted.collect()){
            Assert.assertEquals(String.valueOf(i) + separator + "\"" + String.valueOf(i) + "\\\",try\\\\\"" + separator + "true", line);
            i = i + 1;
        }
    }

    @Test(expected = SparkException.class)
    public void testConvertAllWithJsonNotSupported() {
        log.info("Starts Test convert all with json");
        final String separator = ",";
        final JavaRDD<AvroPayload> payloadData = AvroPayloadUtil.generateTestData(this.jsc.get(), 10, StringTypes.EMPTY);
        final Configuration conf = initConf(separator, "json");
        log.info("Starting to convert data.");
        final FileSinkDataConverter converter = new FileSinkDataConverter(conf, new ErrorExtractor());
        final JavaRDD<String> dataConverted = converter.convertAll(payloadData);
        int i = 1;
        for (String line: dataConverted.collect()){
            Assert.assertEquals(String.valueOf(i) + separator + String.valueOf(i)  + separator + "true", line );
            i = i + 1;
        }
    }

    @Test
    public void testGetHeaderWithCsv() {
        final String separator = ",";
        final JavaRDD<AvroPayload> payloadData = AvroPayloadUtil.generateTestData(this.jsc.get(), 10, StringTypes.EMPTY);
        final Configuration conf = initConf(separator, "csv");
        log.info("Starting to get data header.");
        final FileSinkDataConverter converter = new FileSinkDataConverter(conf, new ErrorExtractor());
        final String header = converter.getHeader(payloadData);
        final String resultHeader = "int_field,string_field,boolean_field";
        Assert.assertEquals(resultHeader, header);
        log.info("Header: {}", header);
    }

    private Configuration initConf(@NonNull final String separator, @NonNull final String fileType) {
        final Configuration conf = new Configuration();
        final String filePrefix = this.fileSystem.get().getWorkingDirectory().toString();
        conf.setProperty(FileSinkConfiguration.PATH_PREFIX, filePrefix);
        conf.setProperty(FileSinkConfiguration.SEPARATOR, separator);
        conf.setProperty(FileSinkConfiguration.FILE_TYPE, fileType);
        conf.setProperty(FileSinkConfiguration.SOURCE_TYPE, "HDFS");
        conf.setProperty(FileSinkConfiguration.TIMESTAMP, "201808011000");
        conf.setProperty(FileSinkConfiguration.PATH_PREFIX, this.fileSystem.get().getWorkingDirectory().toString());
        conf.setProperty(FileSinkConfiguration.SOURCE_NAME_PREFIX, "test.db_test.table");
        return conf;
    }
}
