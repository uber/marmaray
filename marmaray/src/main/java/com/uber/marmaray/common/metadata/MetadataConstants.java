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
package com.uber.marmaray.common.metadata;

import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.exceptions.JobRuntimeException;

public final class MetadataConstants {
    public static final String TEMP_FILE_EXTENSION = ".tmp";
    public static final String CHECKPOINT_KEY = "checkpoint";
    public static final String JOBMANAGER_PREFIX = Configuration.MARMARAY_PREFIX + "jobmanager";
    public static final String JOBMANAGER_METADATA_PREFIX = JOBMANAGER_PREFIX + ".metadata";
    public static final String JOBMANAGER_METADATA_ENABLED = JOBMANAGER_METADATA_PREFIX + ".enabled";
    public static final String JOBMANAGER_METADATA_HDFS_PREFIX = JOBMANAGER_METADATA_PREFIX + ".hdfs";
    public static final String JOBMANAGER_METADATA_HDFS_BASEPATH = JOBMANAGER_METADATA_HDFS_PREFIX + ".basePath";
    public static final String JOBMANAGER_METADATA_STORAGE = JOBMANAGER_METADATA_PREFIX +  ".sourceType";
    public static final String JOBMANAGER_METADATA_SOURCE_HDFS = "HDFS";

    private MetadataConstants() {
        throw new JobRuntimeException("This class should never be instantiated");
    }
}
