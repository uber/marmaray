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

import com.uber.marmaray.common.MetadataManagerType;
import com.uber.marmaray.common.metrics.DataFeedMetrics;
import com.uber.marmaray.common.metrics.IMetricable;
import com.uber.marmaray.common.metrics.JobMetrics;
import com.uber.marmaray.utilities.ConfigUtil;
import lombok.Getter;
import lombok.NonNull;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * {@link MetadataManagerConfiguration} contains all the generic metadata information for where Hive is either a source or
 * sink for the data pipeline job.  All MetadataManagerConfiguration properties starts with {@link #METADATA_MANAGER_PREFIX}.
 */
public class MetadataManagerConfiguration implements Serializable, IMetricable {
    public static final String METADATA_MANAGER_PREFIX = Configuration.MARMARAY_PREFIX + "metadata_manager.";
    public static final String JOB_NAME = METADATA_MANAGER_PREFIX + "job_name";
    public static final String TYPE = METADATA_MANAGER_PREFIX + "type";

    @Getter
    private final Configuration conf;

    @Getter
    private final String jobName;

    @Getter
    private final MetadataManagerType metadataType;

    public MetadataManagerConfiguration(@NonNull final Configuration conf) {
        this.conf = conf;
        ConfigUtil.checkMandatoryProperties(this.conf, getMandatoryProperties());
        this.jobName = this.conf.getProperty(MetadataManagerConfiguration.JOB_NAME).get();
        if (this.conf.getProperty(MetadataManagerConfiguration.TYPE).isPresent()) {
            this.metadataType = MetadataManagerType.valueOf(this.conf.getProperty(MetadataManagerConfiguration.TYPE)
                    .get().trim().toUpperCase());
        }  else {
            this.metadataType = MetadataManagerType.HDFS;
        }
    }

    @Override
    public void setDataFeedMetrics(@NonNull final DataFeedMetrics dataFeedMetrics) {
        // Ignored
    }

    @Override
    public void setJobMetrics(@NonNull final JobMetrics jobMetrics) {
        // Ignored
    }

    public static List<String> getMandatoryProperties() {
        return Collections.unmodifiableList(Arrays.asList(
                MetadataManagerConfiguration.TYPE,
                MetadataManagerConfiguration.JOB_NAME));
    }
}

