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
import com.uber.marmaray.common.util.FileSinkConfigTestUtil;
import org.junit.Assert;
import org.junit.Test;

public class TestAwsConfiguration extends FileSinkConfigTestUtil {

    @Test
    public void testConfigurationWithS3() {
        final Configuration c = initS3("S3");
        final FileSinkConfiguration conf = new FileSinkConfiguration(c);
        final AwsConfiguration awsConf = new AwsConfiguration(conf);
        Assert.assertEquals(conf.getSinkType().name(), "S3" );
        Assert.assertEquals(conf.getAwsLocal(), "/aws_test");
        Assert.assertEquals(awsConf.getBucketName(), "aws-test" );
        Assert.assertEquals(awsConf.getObjectKey(), "marmaray_test/test1" );
        Assert.assertEquals(awsConf.getAwsAccessKeyId(), "username");
        Assert.assertEquals(awsConf.getAwsSecretAccessKey(), "password");
        Assert.assertEquals(awsConf.getRegion(), "us-east-1");
        Assert.assertEquals(awsConf.getSourcePath(), this.fileSystem.get().getWorkingDirectory()+"/aws_test");
    }

    @Test(expected = MissingPropertyException.class)
    public void testConfigurationWithS3MissRegion() {
        final Configuration c = initS3MissConfig("S3", FileSinkConfiguration.AWS_REGION);
        final FileSinkConfiguration conf = new FileSinkConfiguration(c);
        final AwsConfiguration awsConf = new AwsConfiguration(conf);
    }

    @Test(expected = MissingPropertyException.class)
    public void testConfigurationWithS3MissCredentials() {
        final Configuration c = initS3MissConfig("S3", FileSinkConfiguration.AWS_ACCESS_KEY_ID);
        final FileSinkConfiguration conf = new FileSinkConfiguration(c);
        final AwsConfiguration awsConf = new AwsConfiguration(conf);
    }

    @Test(expected = MissingPropertyException.class)
    public void testConfigurationWithS3MissBucketName() {
        final Configuration c = initS3MissConfig("S3", FileSinkConfiguration.BUCKET_NAME);
        final FileSinkConfiguration conf = new FileSinkConfiguration(c);
        final AwsConfiguration awsConf = new AwsConfiguration(conf);
    }

    @Test(expected = MissingPropertyException.class)
    public void testConfigurationWithS3MissObjectKey() {
        final Configuration c = initS3MissConfig("S3", FileSinkConfiguration.OBJECT_KEY);
        final FileSinkConfiguration conf = new FileSinkConfiguration(c);
        final AwsConfiguration awsConf = new AwsConfiguration(conf);
    }
}