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
package com.uber.marmaray.common.util;

import com.uber.marmaray.utilities.FSUtils;
import com.google.common.base.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class TestParquetWriterUtil {
    private FileSystem fileSystem;

    @Before
    public void setupTest() throws IOException {
        /**
         * We explicitly don't call close() in a tearDownTest() method as the Hadoop FileSystem object is cached
         * so if multiple threads are accessing can affect others if one thread closes it.
         */
        this.fileSystem = FSUtils.getFs(new com.uber.marmaray.common.configuration.Configuration(), Optional.absent());
    }

    @Test
    public void writeTestParquetFile() throws IOException {
        final String filePath = FileTestUtil.getTempFolder();
        final Configuration conf = new Configuration();
        final String avscFile = FileHelperUtil.getResourcePath(getClass(),
                new Path("schemas", "StringPair.avsc").toString());

        final Path dataFilePath = new Path(filePath, "data.parquet");

        try (final InputStream fis = new FileInputStream(avscFile)) {
            final Schema.Parser parser = new Schema.Parser();
            final Schema schema = parser.parse(fis);



            final ParquetWriter writer = ParquetWriterUtil.initializeAvroWriter(dataFilePath, schema, conf);
            final GenericRecord record = new GenericData.Record(schema);
            record.put("left", "L");
            record.put("right", "R");

            writer.write(record);
            writer.close();

            Assert.assertTrue(this.fileSystem.exists(dataFilePath));
            FileStatus[] fs = this.fileSystem.globStatus(dataFilePath);

            // Validate that the file isn't empty and data was actually written
            Assert.assertTrue(fs[0].getLen() > 0);
        } finally {
            this.fileSystem.delete(dataFilePath);
        }
    }
}
