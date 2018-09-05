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
package com.uber.marmaray.common.sources.hive;

import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.configuration.HiveSourceConfiguration;
import com.uber.marmaray.common.exceptions.MissingPropertyException;
import org.junit.Assert;
import org.junit.Test;

public class TestHiveSourceConfiguration {

    private static final String JOB_NAME = "jobFoo";
    private static final String DEFAULT_DATA_PATH = "dataPath";
    private static final String DEFAULT_METADATA_PATH = "metadataPath";


    @Test(expected = MissingPropertyException.class)
    public void testMissingHiveDataPath() {
        final Configuration config = new Configuration();
        config.setProperty(HiveSourceConfiguration.JOB_NAME, JOB_NAME);
        final HiveSourceConfiguration hiveConfig = new HiveSourceConfiguration(config);
        Assert.fail();
    }

    @Test(expected = MissingPropertyException.class)
    public void testMissingJobName() {
        final Configuration config = new Configuration();
        config.setProperty(HiveSourceConfiguration.HIVE_DATA_PATH, DEFAULT_DATA_PATH);
        final HiveSourceConfiguration hiveConfig = new HiveSourceConfiguration(config);
        Assert.fail();
    }

    @Test
    public void testBasicConfig() {
        final Configuration config = getValidHiveSourceConfiguration();
        final HiveSourceConfiguration hiveConfig = new HiveSourceConfiguration(config);
        Assert.assertEquals(JOB_NAME, hiveConfig.getJobName());
        Assert.assertEquals(DEFAULT_DATA_PATH, hiveConfig.getDataPath());
        Assert.assertEquals(DEFAULT_METADATA_PATH, hiveConfig.getBaseMetadataPath());
        Assert.assertTrue(hiveConfig.shouldSaveCheckpoint());
    }

    private static Configuration getValidHiveSourceConfiguration() {
        final Configuration config = new Configuration();
        config.setProperty(HiveSourceConfiguration.JOB_NAME, JOB_NAME);
        config.setProperty(HiveSourceConfiguration.HIVE_DATA_PATH, DEFAULT_DATA_PATH);
        config.setProperty(HiveSourceConfiguration.BASE_METADATA_PATH, DEFAULT_METADATA_PATH);
        return config;
    }
}

