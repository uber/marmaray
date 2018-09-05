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
import com.uber.marmaray.common.configuration.FileSinkConfiguration;
import lombok.NonNull;

public class FileSinkConfigTestUtil extends AbstractSparkTest {
    protected Configuration initS3(@NonNull final String sinkType) {
        final Configuration conf = initCommon("/new_path/new_test", "csv", 1, ",", sinkType, "version");
        conf.setProperty(FileSinkConfiguration.AWS_LOCAL, "/aws_test");
        conf.setProperty(FileSinkConfiguration.AWS_REGION, "us-east-1");
        conf.setProperty(FileSinkConfiguration.AWS_ACCESS_KEY_ID, "username");
        conf.setProperty(FileSinkConfiguration.AWS_SECRET_ACCESS_KEY, "password");
        conf.setProperty(FileSinkConfiguration.BUCKET_NAME, "aws-test");
        conf.setProperty(FileSinkConfiguration.OBJECT_KEY, "marmaray_test/test1");
        return conf;
    }

    protected Configuration initS3MissConfig(@NonNull final String sinkType, @NonNull final String propertyExclude) {
        final Configuration conf = initCommon("/new_path/new_test", "csv", 1, ",", sinkType, "version");
        conf.setProperty(FileSinkConfiguration.AWS_LOCAL, "/aws_test");
        if (!propertyExclude.equals(FileSinkConfiguration.AWS_REGION)) {
            conf.setProperty(FileSinkConfiguration.AWS_REGION, "us-east-1");
        }
        if (!propertyExclude.equals(FileSinkConfiguration.AWS_ACCESS_KEY_ID)) {
            conf.setProperty(FileSinkConfiguration.AWS_ACCESS_KEY_ID, "username");
            conf.setProperty(FileSinkConfiguration.AWS_SECRET_ACCESS_KEY, "password");
        }
        if (!propertyExclude.equals(FileSinkConfiguration.BUCKET_NAME)) {
            conf.setProperty(FileSinkConfiguration.BUCKET_NAME, "aws-test");
        }
        if (!propertyExclude.equals(FileSinkConfiguration.OBJECT_KEY)) {
            conf.setProperty(FileSinkConfiguration.OBJECT_KEY, "marmaray_test/test1");
        }
        return conf;
    }

    protected Configuration initCommon(@NonNull final String path, @NonNull final String fileType,
                                     @NonNull final long fileMB, @NonNull final String separator,
                                       @NonNull final String sinkType, @NonNull final String dispersalType) {
        final Configuration c = initFileNameAndPath("date", true, dispersalType);
        c.setProperty(FileSinkConfiguration.FS_PATH, path);
        c.setProperty(FileSinkConfiguration.FILE_TYPE, fileType);
        c.setProperty(FileSinkConfiguration.FILE_SIZE_MEGABYTE, String.valueOf(fileMB));
        c.setProperty(FileSinkConfiguration.SEPARATOR, separator);
        c.setProperty(FileSinkConfiguration.FILE_SINK_TYPE, sinkType);
        return c;
    }

    protected Configuration initFileNameAndPath(@NonNull final String partitionType, @NonNull final boolean sourceSubPathExists,
                                                @NonNull final String dispersalType) {
        final Configuration c = initFileSystem();
        c.setProperty(FileSinkConfiguration.SOURCE_TYPE, "hive");
        c.setProperty(FileSinkConfiguration.TIMESTAMP, "201808011025");
        c.setProperty(FileSinkConfiguration.SOURCE_NAME_PREFIX, "test.db_test_trip.table");
        c.setProperty(FileSinkConfiguration.DISPERSAL_TYPE, dispersalType);
        if (sourceSubPathExists) {
            c.setProperty(FileSinkConfiguration.SOURCE_PARTITION_PATH, "2018/08/01");
        }
        c.setProperty(FileSinkConfiguration.PARTITION_TYPE, partitionType);
        return c;
    }

    protected Configuration initFileNameAndPathMissConfig(@NonNull final String propertyExclude) {
        Configuration c = new Configuration();
        if (!propertyExclude.equals(FileSinkConfiguration.PATH_PREFIX)) {
            c = initFileSystem();
        }
        if (!propertyExclude.equals(FileSinkConfiguration.SOURCE_TYPE)) {
            c.setProperty(FileSinkConfiguration.SOURCE_TYPE, "hive");
        }
        if (!propertyExclude.equals(FileSinkConfiguration.TIMESTAMP)) {
            c.setProperty(FileSinkConfiguration.TIMESTAMP, "201808011025");
        }
        if (!propertyExclude.equals(FileSinkConfiguration.SOURCE_NAME_PREFIX)) {
            c.setProperty(FileSinkConfiguration.SOURCE_NAME_PREFIX, "test.db_test_trip.table");
        }
        c.setProperty(FileSinkConfiguration.DISPERSAL_TYPE, "version");
        c.setProperty(FileSinkConfiguration.SOURCE_PARTITION_PATH, "2018/08/01");
        c.setProperty(FileSinkConfiguration.PARTITION_TYPE, "date");
        return c;
    }

    protected Configuration initFileSystem() {
        final Configuration c = new Configuration();
        final String filePrefix = this.fileSystem.get().getWorkingDirectory().toString();
        c.setProperty(FileSinkConfiguration.PATH_PREFIX, filePrefix);
        return c;
    }
}
