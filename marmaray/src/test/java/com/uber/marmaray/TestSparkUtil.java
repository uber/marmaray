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

package com.uber.marmaray;

import com.google.common.base.Optional;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.configuration.TestConfiguration;
import com.uber.marmaray.common.spark.SparkArgs;
import com.uber.marmaray.common.spark.SparkFactory;
import java.io.File;
import java.util.Collections;
import org.apache.spark.SparkConf;
import org.junit.Assert;
import org.junit.Test;

public class TestSparkUtil {

    @Test
    public void testSparkConfOverride() {
        final Configuration conf = new Configuration(
            TestSparkUtil.class.getResourceAsStream("/configWithScopes.yaml"),
            Optional.of("incremental"));

        final SparkConf sparkConf = getSparkConf(conf);
        Assert.assertEquals("4g", sparkConf.get("spark.executor.memory"));
        Assert.assertEquals("4g", sparkConf.get("spark.driver.memory"));
        Assert.assertEquals("100s", sparkConf.get("spark.network.timeout"));
    }

    private SparkConf getSparkConf(final Configuration conf) {
        final SparkArgs sparkArgs = new SparkArgs(Collections.emptyList(), Collections.emptyList(), conf);
        final SparkFactory sparkFactory = new SparkFactory(sparkArgs);
        return sparkFactory.createSparkConf();
    }

    @Test
    public void testSparkConfOverrideDoesNotFailWithoutAnySparkConfDefinitions() {
        final Configuration conf = new Configuration(new File(TestConfiguration.CONFIG_YAML),
            Optional.absent());
        getSparkConf(conf);
    }
}
