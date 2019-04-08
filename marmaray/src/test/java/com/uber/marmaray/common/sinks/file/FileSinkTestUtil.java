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

package com.uber.marmaray.common.sinks.file;

import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.configuration.FileSinkConfiguration;

import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.util.AbstractSparkTest;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import java.nio.file.FileSystemNotFoundException;

@Slf4j
public class FileSinkTestUtil extends AbstractSparkTest {
    protected String pathPrefix;

    @Before
    public void setupTest() {
        super.setupTest();
        try {
            if (this.fileSystem.isPresent()) {
                this.pathPrefix = this.fileSystem.get().getWorkingDirectory().toString();
            }
            else {
                throw new FileSystemNotFoundException("File System not found.");
            }

        } catch (FileSystemNotFoundException e) {
            log.error("Exception: {}", e.getMessage());
            throw new JobRuntimeException(e);
        }
    }

    protected Configuration initConfig(@NonNull final String pathPrefix, @NonNull final String fsPath,
                                       @NonNull final String separator, @NonNull final String timeStamp,
                                       @NonNull final String sourceSubPath, @NonNull final String dispersalType) {
        final Configuration conf=new Configuration();
        conf.setProperty(FileSinkConfiguration.FS_PATH,fsPath);
        conf.setProperty(FileSinkConfiguration.FILE_TYPE,"csv");
        conf.setProperty(FileSinkConfiguration.FILE_SIZE_MEGABYTE,"0.001");
        conf.setProperty(FileSinkConfiguration.SEPARATOR,separator);
        conf.setProperty(FileSinkConfiguration.PATH_PREFIX, pathPrefix);
        conf.setProperty(FileSinkConfiguration.SOURCE_TYPE, "hive");
        conf.setProperty(FileSinkConfiguration.TIMESTAMP, timeStamp);
        conf.setProperty(FileSinkConfiguration.SOURCE_NAME_PREFIX, "test.db_test_trip.table");
        conf.setProperty(FileSinkConfiguration.DISPERSAL_TYPE, dispersalType);
        conf.setProperty(FileSinkConfiguration.SOURCE_PARTITION_PATH, sourceSubPath);
        conf.setProperty(FileSinkConfiguration.PARTITION_TYPE, "date");
        return conf;
    }

    protected Configuration initConfigWithAws(@NonNull final String pathPrefix, @NonNull final String objectKey,
                                              @NonNull final String awsLocal, @NonNull final String dispersalType,
                                              @NonNull final String timeStamp, @NonNull final String sourceSubPath,
                                              @NonNull final String bucketName, @NonNull final String fileType) {
        final Configuration conf=new Configuration();
        conf.setProperty(FileSinkConfiguration.FILE_TYPE,fileType);
        conf.setProperty(FileSinkConfiguration.FILE_SIZE_MEGABYTE,"0.001");
        conf.setProperty(FileSinkConfiguration.SEPARATOR,",");
        conf.setProperty(FileSinkConfiguration.PATH_PREFIX, pathPrefix);
        conf.setProperty(FileSinkConfiguration.SOURCE_TYPE, "hive");
        conf.setProperty(FileSinkConfiguration.TIMESTAMP, timeStamp);
        conf.setProperty(FileSinkConfiguration.SOURCE_NAME_PREFIX, "test.db_test_trip.table");
        conf.setProperty(FileSinkConfiguration.DISPERSAL_TYPE, dispersalType);
        conf.setProperty(FileSinkConfiguration.SOURCE_PARTITION_PATH, sourceSubPath);
        conf.setProperty(FileSinkConfiguration.PARTITION_TYPE, "date");
        //Aws Property
        conf.setProperty(FileSinkConfiguration.FILE_SINK_TYPE, "S3");
        conf.setProperty(FileSinkConfiguration.AWS_LOCAL, awsLocal);
        conf.setProperty(FileSinkConfiguration.BUCKET_NAME, bucketName);
        conf.setProperty(FileSinkConfiguration.OBJECT_KEY, "marmaray_test/" + objectKey);
        conf.setProperty(FileSinkConfiguration.AWS_REGION, "us-east-1");
        conf.setProperty(FileSinkConfiguration.AWS_ACCESS_KEY_ID, "username");
        conf.setProperty(FileSinkConfiguration.AWS_SECRET_ACCESS_KEY, "password");
        return conf;
    }
}
