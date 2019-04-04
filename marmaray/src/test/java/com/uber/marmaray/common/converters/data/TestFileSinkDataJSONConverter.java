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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Assert;
import org.junit.Test;

@Slf4j
public class TestFileSinkDataJSONConverter extends AbstractSparkTest {

    private static final String SEQUENCE = "sequence";

    @Test
    public void testConvertAll() {
        log.info("Starts Test json convert");
        final JavaRDD<AvroPayload> payloadData = AvroPayloadUtil.generateTestData(this.jsc.get(), 5, StringTypes.EMPTY);
        final Configuration conf = initConf(SEQUENCE);
        log.info("Starting to convert data.");
        final FileSinkDataJSONConverter converter = new FileSinkDataJSONConverter(conf, new ErrorExtractor());
        final JavaPairRDD<String, String> tmpDataConverted = converter.convertAll(payloadData);
        final JavaRDD<String> dataConverted = tmpDataConverted.map(message -> message._2());
        int i = 1;
        for (String line: dataConverted.collect()){
            log.info("line {} : {}", i, line);
            //samlpe json : {"int_field":"1","boolean_field":"true","string_field":"1"}
            Assert.assertEquals("{\"int_field\":\"" + String.valueOf(i)
                    + "\",\"boolean_field\":\"true\",\"string_field\":\"" + String.valueOf(i) +"\"}", line);
            i = i + 1;
        }
    }

    private Configuration initConf(@NonNull final String fileType) {
        final Configuration conf = new Configuration();
        final String filePrefix = this.fileSystem.get().getWorkingDirectory().toString();
        conf.setProperty(FileSinkConfiguration.PATH_PREFIX, filePrefix);
        conf.setProperty(FileSinkConfiguration.FILE_TYPE, fileType);
        conf.setProperty(FileSinkConfiguration.COMPRESSION_CODEC, "lz4");
        conf.setProperty(FileSinkConfiguration.SOURCE_TYPE, "HDFS");
        conf.setProperty(FileSinkConfiguration.TIMESTAMP, "201811280000");
        conf.setProperty(FileSinkConfiguration.PATH_PREFIX, this.fileSystem.get().getWorkingDirectory().toString());
        conf.setProperty(FileSinkConfiguration.SOURCE_NAME_PREFIX, "test.db_test.table");
        return conf;
    }
}