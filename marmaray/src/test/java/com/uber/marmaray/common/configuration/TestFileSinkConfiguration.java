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

package com.uber.marmaray.common.configuration;

import com.uber.marmaray.common.exceptions.MissingPropertyException;
import com.uber.marmaray.common.util.AbstractSparkTest;
import com.uber.marmaray.common.util.FileSinkConfigTestUtil;
import lombok.NonNull;
import org.apache.hadoop.hdfs.DFSClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import sun.security.krb5.Config;

import static org.junit.Assert.*;

public class TestFileSinkConfiguration extends FileSinkConfigTestUtil {
    @Test
    public void testDefaultConfigurationInitialized() {
        final Configuration c = initFileNameAndPath("date", true, "version");
        final FileSinkConfiguration conf = new FileSinkConfiguration(c);
        Assert.assertEquals(conf.getSeparator(), conf.DEFAULT_SEPARATOR);
        Assert.assertEquals(conf.getFileSizeMegaBytes(), conf.DEFAULT_FILE_SIZE, 0);
        Assert.assertEquals(conf.getFileType(), conf.DEFAULT_FILE_TYPE);
    }

    @Test
    public void testDefinedConfigurationInitialized() {
        final Configuration c = initCommon("/newpath/newtest", "json", 1500, " ", "HDFS", "version");
        final FileSinkConfiguration conf = new FileSinkConfiguration(c);
        Assert.assertEquals(conf.getSeparator(), ' ');
        Assert.assertEquals(conf.getFileSizeMegaBytes(), 1500, 0);
        Assert.assertEquals(conf.getFileType(), "json");
        Assert.assertEquals(conf.getSourcePartitionPath().get(), "2018/08/01");
        Assert.assertEquals(conf.getSourceType(), "hive");
        Assert.assertEquals(conf.getSourceNamePrefix(), "test.db_test_trip.table");
        Assert.assertEquals(conf.getPartitionType().name(), "DATE");
        Assert.assertEquals(conf.getFileNamePrefix(), "marmaray_hive_test.db_test_trip.table_201808011025");
        Assert.assertEquals(conf.getFullPath(), this.fileSystem.get().getWorkingDirectory() + "/newpath/newtest/2018/08/01/" + conf.getFileNamePrefix() );
        Assert.assertEquals(conf.getPathHdfs(), this.fileSystem.get().getWorkingDirectory() + "/newpath/newtest/2018/08/01");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testWithConfSeparatorNotSupported() {
        final Configuration c = initCommon("/newpath/newtest", "json", 1500, " ,", "HDFS", "version");
        final FileSinkConfiguration conf = new FileSinkConfiguration(c);
    }

    @Test(expected = MissingPropertyException.class)
    public void testWithConfMissPartitionType() {
        final Configuration c = initFileNameAndPathMissConfig(FileSinkConfiguration.PATH_PREFIX);
        final FileSinkConfiguration conf = new FileSinkConfiguration(c);
    }

    @Test(expected = MissingPropertyException.class)
    public void testWithConfMissSourceDataPath() {
        final Configuration c = initFileNameAndPathMissConfig(FileSinkConfiguration.SOURCE_NAME_PREFIX);
        final FileSinkConfiguration conf = new FileSinkConfiguration(c);
    }

    @Test(expected = MissingPropertyException.class)
    public void testWithConfMissTimeStamp() {
        final Configuration c = initFileNameAndPathMissConfig(FileSinkConfiguration.TIMESTAMP);
        final FileSinkConfiguration conf = new FileSinkConfiguration(c);
    }

    @Test(expected = MissingPropertyException.class)
    public void testWithConfMissSourceType() {
        final Configuration c = initFileNameAndPathMissConfig(FileSinkConfiguration.SOURCE_TYPE);
        final FileSinkConfiguration conf = new FileSinkConfiguration(c);
    }

    @Test(expected = MissingPropertyException.class)
    public void testWithPartitionTypeButMissSourceSubPath() {
        final Configuration c = initFileNameAndPath("date", false, "version");
        final FileSinkConfiguration conf = new FileSinkConfiguration(c);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testWithPartitionTypeNoneButPartitionKeyStr() {
        final Configuration c = initFileNameAndPath("none", false, "version");
        c.setProperty(HiveConfiguration.PARTITION_KEY_NAME, "datestr");
        final FileSinkConfiguration conf = new FileSinkConfiguration(c);
    }

    @Test
    public void testConfigurationWithS3() {
        final Configuration c = initS3("S3");
        //Aws Property
        final FileSinkConfiguration conf = new FileSinkConfiguration(c);
        Assert.assertEquals(conf.getSinkType().name(), "S3" );
        Assert.assertEquals(conf.getAwsLocal(), "/aws_test");
        Assert.assertEquals(conf.getBucketName().get(), "aws-test" );
        Assert.assertEquals(conf.getObjectKey().get(), "marmaray_test/test1" );
        Assert.assertEquals(conf.getFullPath(), this.fileSystem.get().getWorkingDirectory()+"/aws_test");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testConfWithS3SinkTypeNotSupported() {
        final Configuration c = initCommon("/newpath/newtest", "csv", 1500, " ", "LOCAL", "version");
        final FileSinkConfiguration conf = new FileSinkConfiguration(c);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testConfWithS3DispersalTypeNotSupported() {
        final Configuration c = initCommon("/newpath/newtest", "csv", 1500, " ", "hdfs", "unknownType");
        final FileSinkConfiguration conf = new FileSinkConfiguration(c);
    }
}
