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

import com.google.common.base.Optional;
import com.uber.marmaray.common.PartitionType;
import com.uber.marmaray.common.metadata.StringValue;
import com.uber.marmaray.utilities.ConfigUtil;
import lombok.Getter;
import lombok.NonNull;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * {@link HiveConfiguration} contains all the generic metadata information for where Hive is either a source or
 * sink for the data pipeline job.  All HiveConfiguration properties starts with {@link #HIVE_PROPERTY_PREFIX}.
 */
public class HiveConfiguration implements Serializable {
    public static final String HIVE_PROPERTY_PREFIX = Configuration.MARMARAY_PREFIX + "hive.";
    public static final String HIVE_DATA_PATH = HIVE_PROPERTY_PREFIX + "data_path";
    public static final String JOB_NAME = HIVE_PROPERTY_PREFIX + "job_name";
    public static final String PARTITION_KEY_NAME = HIVE_PROPERTY_PREFIX + "partition_key_name";
    public static final String PARTITION_TYPE = HIVE_PROPERTY_PREFIX + "partition_type";
    public static final String PARTITION = HIVE_PROPERTY_PREFIX + "partition";

    @Getter
    private final Configuration conf;

    /**
     * This is the path where the data is either dispersed to (for sink) or read from (source) depending on context
     */
    @Getter
    private final String dataPath;

    @Getter
    private final String jobName;

    @Getter
    private final Optional<String> partitionKeyName;

    @Getter
    private final Optional<StringValue> partition;

    @Getter
    private final PartitionType partitionType;

    public HiveConfiguration(@NonNull final Configuration conf) {
        this.conf = conf;
        ConfigUtil.checkMandatoryProperties(this.conf, getMandatoryProperties());

        this.dataPath = this.getConf().getProperty(HIVE_DATA_PATH).get();
        this.jobName = this.getConf().getProperty(JOB_NAME).get();
        this.partitionKeyName = this.getConf().getProperty(PARTITION_KEY_NAME);

        if (this.conf.getProperty(PARTITION_TYPE).isPresent()) {
            this.partitionType = PartitionType.valueOf(this.getConf().getProperty(PARTITION_TYPE)
                    .get().trim().toUpperCase());
        } else {
            this.partitionType = PartitionType.NONE;
        }

        this.partition = this.getConf().getProperty(PARTITION).isPresent()
                ? Optional.of(new StringValue(this.getConf().getProperty(PARTITION).get())) : Optional.absent();
    }

    public static List<String> getMandatoryProperties() {
        return Collections.unmodifiableList(Arrays.asList(HIVE_DATA_PATH, JOB_NAME));
    }
}
