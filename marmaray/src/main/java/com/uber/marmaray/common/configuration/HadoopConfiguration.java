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

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link HadoopConfiguration} uses system specific hadoop configurations and overrides them with the hadoop specific
 * configs defined in {@link Configuration} which starts with {@link #HADOOP_COMMON_PROPERTY_PREFIX}.
 */
@Slf4j
@RequiredArgsConstructor
public class HadoopConfiguration {
    public static final String HADOOP_COMMON_PROPERTY_PREFIX = Configuration.MARMARAY_PREFIX + "hadoop.";

    // Hadoop properties not defined in our yaml configuration and set dynamically based on cluster topology
    public static final String HADOOP_DEFAULT_FS = "fs.defaultFS";

    @Getter
    private final Configuration conf;

    /**
     * Returns hadoop configuration.
     */
    public org.apache.hadoop.conf.Configuration getHadoopConf() {
        final org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        this.conf.getPropertiesWithPrefix(HADOOP_COMMON_PROPERTY_PREFIX, true).forEach(
            (key, value) -> {
                log.info("hadoop-conf-update:key:[{}]:value:[{}]", key, value);
                hadoopConf.set(key, value);
            }
        );
        return hadoopConf;
    }
}
