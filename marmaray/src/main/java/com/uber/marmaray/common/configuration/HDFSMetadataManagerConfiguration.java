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
import lombok.NonNull;

/**
 * {@link HDFSMetadataManagerConfiguration} contains all the generic metadata information for where Hive is either a source or
 * sink for the data pipeline job.  All HDFSMetadataManagerConfiguration properties starts with {@link #}.
 */
public class HDFSMetadataManagerConfiguration extends MetadataManagerConfiguration {
    public static final String HDFS_METADATA_MANAGER_PREFIX = METADATA_MANAGER_PREFIX + "HDFS.";
    public static final String BASE_METADATA_PATH = HDFS_METADATA_MANAGER_PREFIX + "job_metadata";

    @Getter
    private final String baseMetadataPath;

    public HDFSMetadataManagerConfiguration(@NonNull final Configuration conf) {
        super(conf);
        this.baseMetadataPath = this.getConf().getProperty(BASE_METADATA_PATH).get();
    }
}

