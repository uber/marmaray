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

import com.uber.marmaray.utilities.ErrorTableUtil;

import org.hibernate.validator.constraints.NotEmpty;
import org.junit.Assert;
import org.junit.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestErrorTableConfiguration {
    private static final String TARGET_TABLE = "target_table";
    private static final String ERROR_TABLE = "error_table";
    final String tableName = "table1";
    final String errorTableName = tableName + ErrorTableUtil.ERROR_TABLE_SUFFIX;
    final String basePath = "/tmp/basePath";
    final String errorBasePath = "/tmp/basePath/.error_table";
    final String metricsPrefix = "test_metrics";
    final String errorMetricsPrefix = metricsPrefix + ErrorTableUtil.ERROR_TABLE_SUFFIX;
    final String testSchema = "TEST_SCHEMA";

    @Test
    public void testErrorTableConfiguration() {
        final String hoodieTableNameKey = HoodieConfiguration.getTablePropertyKey(HoodieConfiguration.HOODIE_TABLE_NAME, TARGET_TABLE);
        final String metricsPrefixKey = HoodieConfiguration.getTablePropertyKey(HoodieConfiguration.HOODIE_METRICS_PREFIX, TARGET_TABLE);
        final String base = HoodieConfiguration.getTablePropertyKey(HoodieConfiguration.HOODIE_BASE_PATH, TARGET_TABLE);

        Configuration conf = new Configuration();
        conf.setProperty(hoodieTableNameKey, tableName);
        conf.setProperty(metricsPrefixKey, metricsPrefix);
        conf.setProperty(ErrorTableConfiguration.IS_ENABLED, "true");
        conf.setProperty(ErrorTableConfiguration.DESTINATION_PATH, errorBasePath);
        conf.setProperty(base, basePath);

        HoodieConfiguration hoodieConfiguration = createHoodieConfiguration(conf);

        Assert.assertEquals(hoodieConfiguration.getBasePath(), errorBasePath);
        Assert.assertEquals(hoodieConfiguration.getTableName(), errorTableName);
        Assert.assertEquals(hoodieConfiguration.getHoodieMetricsPrefix(), errorMetricsPrefix);
    }

    private HoodieConfiguration createHoodieConfiguration(@NotEmpty final Configuration conf) {
        ErrorTableConfiguration errorTableConfiguration = new ErrorTableConfiguration(conf);
        HoodieConfiguration hoodieConfiguration = errorTableConfiguration.getHoodieConfiguration(conf, testSchema,
            TARGET_TABLE, ERROR_TABLE, true);
        return hoodieConfiguration;
    }
}
