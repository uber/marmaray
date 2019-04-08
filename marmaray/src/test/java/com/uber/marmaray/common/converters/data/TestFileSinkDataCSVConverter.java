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
import com.uber.marmaray.common.converters.converterresult.ConverterResult;
import com.uber.marmaray.common.util.AbstractSparkTest;
import com.uber.marmaray.common.util.AvroPayloadUtil;
import com.uber.marmaray.utilities.ErrorExtractor;
import com.uber.marmaray.utilities.SchemaUtil;
import com.uber.marmaray.utilities.StringTypes;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.List;
import java.util.TimeZone;


@Slf4j
public class TestFileSinkDataCSVConverter extends AbstractSparkTest {

    private static final String SEPARATOR = ",";
    private static final String CSV = "csv";

    @Test
    public void testConvertAllWithCsv() {
        log.info("Starts Test convert all with csv");
        final JavaRDD<AvroPayload> payloadData = AvroPayloadUtil.generateTestData(this.jsc.get(), 10, StringTypes.EMPTY);
        final Configuration conf = initConf(SEPARATOR, CSV);
        log.info("Starting to convert data.");
        final FileSinkDataCSVConverter converter = new FileSinkDataCSVConverter(conf, new ErrorExtractor());
        final JavaPairRDD<String, String> tmpDataConverted = converter.convertAll(payloadData);
        final JavaRDD<String> dataConverted = tmpDataConverted.map(message -> message._2());
        int i = 1;
        for (String line: dataConverted.collect()){
            Assert.assertEquals(String.valueOf(i) + SEPARATOR + String.valueOf(i)  + SEPARATOR + "true", line );
            i = i + 1;
        }
    }

    @Test
    public void testConvertAllWithCsvSpecialChar() {
        log.info("Starts Test convert all with csv");
        final String separator = ",";
        final JavaRDD<AvroPayload> payloadData = AvroPayloadUtil.generateTestDataNew(this.jsc.get(), 10, StringTypes.EMPTY);
        final Configuration conf = initConf(separator, CSV);
        log.info("Starting to convert data.");
        final FileSinkDataCSVConverter converter = new FileSinkDataCSVConverter(conf, new ErrorExtractor());
        final JavaPairRDD<String, String> tmpDataConverted = converter.convertAll(payloadData);
        final JavaRDD<String> dataConverted = tmpDataConverted.map(message -> message._2());
        int i = 1;
        for (String line: dataConverted.collect()){
            Assert.assertEquals(String.valueOf(i) + SEPARATOR + "\"" + String.valueOf(i) + "\\\",try\\\\\"" + SEPARATOR + "true", line);
            i = i + 1;
        }
    }

    @Test(expected = SparkException.class)
    public void testConvertAllWithJsonNotSupported() {
        log.info("Starts Test convert all with json");
        final JavaRDD<AvroPayload> payloadData = AvroPayloadUtil.generateTestData(this.jsc.get(), 10, StringTypes.EMPTY);
        final Configuration conf = initConf(SEPARATOR, "json");
        log.info("Starting to convert data.");
        final FileSinkDataCSVConverter converter = new FileSinkDataCSVConverter(conf, new ErrorExtractor());
        final JavaPairRDD<String, String> tmpDataConverted = converter.convertAll(payloadData);
        final JavaRDD<String> dataConverted = tmpDataConverted.map(message -> message._2());
        int i = 1;
        for (String line: dataConverted.collect()){
            Assert.assertEquals(String.valueOf(i) + SEPARATOR + String.valueOf(i)  + SEPARATOR + "true", line );
            i = i + 1;
        }
    }

    @Test
    public void testConvertTimestamp() {
        final TimeZone defaultTZ = TimeZone.getDefault();
        try {
            // test is assuming UTC
            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
            final Schema schema = SchemaBuilder.builder().record("mySchema").fields()
                    .name("ts").type(SchemaUtil.getTimestampSchema(true)).noDefault()
                    .name("longId").type().nullable().longType().noDefault()
                    .endRecord();
            GenericRecord gr = new GenericData.Record(schema);
            gr.put("ts", SchemaUtil.encodeTimestamp(new Timestamp(1514844885123L)));
            gr.put("longId", 123456L);
            final Configuration conf = initConf(SEPARATOR, CSV);
            AvroPayload payloadData = new AvroPayload(gr);
            final FileSinkDataCSVConverter converter = new FileSinkDataCSVConverter(conf, new ErrorExtractor());
            List<ConverterResult<AvroPayload, String>> converted = converter.convert(payloadData);
            Assert.assertEquals(1, converted.size());
            Assert.assertTrue(converted.get(0).getSuccessData().isPresent());
            Assert.assertEquals("2018-01-01 22:14:45.123 +0000,123456",
                    converted.get(0).getSuccessData().get().getData());
            gr = new GenericData.Record(schema);
            gr.put("longId", 123456L);
            payloadData = new AvroPayload(gr);
            converted = converter.convert(payloadData);
            Assert.assertEquals(1, converted.size());
            Assert.assertTrue(converted.get(0).getSuccessData().isPresent());
            Assert.assertEquals(",123456",
                    converted.get(0).getSuccessData().get().getData());
            gr = new GenericData.Record(schema);
            payloadData = new AvroPayload(gr);
            converted = converter.convert(payloadData);
            Assert.assertEquals(1, converted.size());
            Assert.assertTrue(converted.get(0).getSuccessData().isPresent());
            Assert.assertEquals(",",
                    converted.get(0).getSuccessData().get().getData());

        } finally {
            TimeZone.setDefault(defaultTZ);
        }
    }

    @Test
    public void testGetHeaderWithCsv() {
        final JavaRDD<AvroPayload> payloadData = AvroPayloadUtil.generateTestData(this.jsc.get(), 10, StringTypes.EMPTY);
        final Configuration conf = initConf(SEPARATOR, CSV);
        log.info("Starting to get data header.");
        final FileSinkDataConverter converter = new FileSinkDataCSVConverter(conf, new ErrorExtractor());
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

    @Test
    public void testBinary() {
        final Schema schema = SchemaBuilder.builder().record("mySchema").fields()
                .name("byteField").type().nullable().bytesType().noDefault()
                .endRecord();
        final GenericRecord gr = new GenericData.Record(schema);
        gr.put("byteField", ByteBuffer.wrap(new byte[] {0x12, 0x34, 0x56, 0x78, 0x0a}));
        final Configuration conf = initConf(SEPARATOR, CSV);
        final AvroPayload payloadData = new AvroPayload(gr);
        final FileSinkDataCSVConverter converter = new FileSinkDataCSVConverter(conf, new ErrorExtractor());
        final List<ConverterResult<AvroPayload, String>> converted = converter.convert(payloadData);
        Assert.assertEquals(1, converted.size());
        Assert.assertTrue(converted.get(0).getSuccessData().isPresent());
        Assert.assertEquals("0x123456780a", converted.get(0).getSuccessData().get().getData());
    }
}
