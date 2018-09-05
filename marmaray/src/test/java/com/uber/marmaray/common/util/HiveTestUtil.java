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
import com.uber.marmaray.common.configuration.HiveSourceConfiguration;

public final class HiveTestUtil {
    private HiveTestUtil() {
        throw new RuntimeException("This test class should never be instantiated");
    }

    public static HiveSourceConfiguration initializeConfig(final String jobName,
                                                           final String dataPath,
                                                           final String metadataPath)
    {
        final Configuration config = new Configuration();
        config.setProperty(HiveSourceConfiguration.JOB_NAME, jobName);
        config.setProperty(HiveSourceConfiguration.HIVE_DATA_PATH, dataPath);
        config.setProperty(HiveSourceConfiguration.BASE_METADATA_PATH, metadataPath);
        return new HiveSourceConfiguration(config);
    }
}
