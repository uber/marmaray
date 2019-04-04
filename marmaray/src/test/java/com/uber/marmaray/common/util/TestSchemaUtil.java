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

import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.utilities.FSUtils;
import com.uber.marmaray.utilities.SchemaUtil;
import com.google.common.base.Optional;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TestSchemaUtil {

    private FileSystem fs;

    @Before
    public void setupTest() throws IOException {
        this.fs = FSUtils.getFs(new Configuration(), Optional.absent());
    }

    @Test
    public void testGenerateSchemaFromParquet() throws IOException {
        final String parquetDataPath = FileHelperUtil.getResourcePath(getClass(),
            new Path("testData", "testPartition").toString());
        final StructType structType = SchemaUtil.generateSchemaFromParquet(this.fs,
                parquetDataPath, Optional.absent());
        Assert.assertEquals(2, structType.fields().length);
        validate(structType);
    }

    @Test
    public void testMultiPartitionSchema() throws Exception {
        final String parquetDataPath = FileHelperUtil.getResourcePath(getClass(),
            new Path("testData", "testPartition1").toString());
        final StructType structType = SchemaUtil.generateSchemaFromParquet(this.fs,
                parquetDataPath, Optional.absent());
        Assert.assertEquals(2, structType.fields().length);
        validate(structType);
    }

    private void validate(final StructType structType) {
        int i = 0;

        for (StructField f : structType.fields()) {
            if (i == 0) {
                Assert.assertEquals("left", f.name());
            } else if (i == 1) {
                Assert.assertEquals("right", f.name());
            }
            i++;
        }
    }

}
