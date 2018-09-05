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

import com.google.common.base.Optional;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.utilities.FSUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestHDFSJobLevelMetadataTracker {

    private static final String JOB_NAME = "testJobMetadataManager";
    private static final String TEST_TOPIC_1 = "topic1";
    private static final String TEST_TOPIC_2 = "topic2";
    private Path basePath;
    private FileSystem fileSystem;
    private Configuration config;
    private Optional<JobManagerMetadataTracker> tracker;

    @Before
    public void setupTest() throws IOException {
        config = new Configuration();
        config.setProperty(MetadataConstants.JOBMANAGER_METADATA_STORAGE, "hdfs");
        config.setProperty(MetadataConstants.JOBMANAGER_METADATA_HDFS_BASEPATH,
                HDFSTestConstants.JOBMANAGER_BASE_METADATA_BASEPATH);
        this.fileSystem = FSUtils.getFs(config);
        final AtomicBoolean condition = new AtomicBoolean(true);
        basePath = new Path(HDFSTestConstants.JOBMANAGER_BASE_METADATA_BASEPATH);
        this.tracker = Optional.of(new JobManagerMetadataTracker(config));
    }

    @Test
    public void testReadWriteJobManagerMetadata() throws IOException {
        final Map<String, String> testData1 = new HashMap<String, String>();
        testData1.put("metaKey1","metaValue1");
        testData1.put("metaKey2","metaValue2");
        this.tracker.get().set(TEST_TOPIC_1, testData1 );
        final Map<String, String> testData2 = new HashMap<String, String>();
        testData2.put("metaKey1","metaValue1");
        testData2.put("metaKey2","metaValue2");
        this.tracker.get().set(TEST_TOPIC_2, testData2);
        this.tracker.get().writeJobManagerMetadata();
        final FileStatus[] statuses = fileSystem.listStatus(basePath);
        Assert.assertEquals(1,fileSystem.listStatus(basePath).length);

        this.tracker = Optional.absent();
        this.tracker = Optional.of(new JobManagerMetadataTracker(config));

        Assert.assertEquals(this.tracker.get().contains(TEST_TOPIC_1), true);
        Assert.assertEquals(this.tracker.get().contains(TEST_TOPIC_2), true);
        Assert.assertEquals(this.tracker.get().get(TEST_TOPIC_2).get().keySet().size(),2);
        Assert.assertEquals(this.tracker.get().get(TEST_TOPIC_1).get().keySet().size(),2);
        Assert.assertEquals(this.tracker.get().get(TEST_TOPIC_1).get().get("metaKey1").toString(),"metaValue1");
        Assert.assertEquals(this.tracker.get().get(TEST_TOPIC_1).get().get("metaKey1").toString(),"metaValue1");
    }

    @After
    public void clearTestSetup() throws IOException {
        this.fileSystem.delete(basePath, true);

        if (this.fileSystem != null) {
            this.fileSystem.close();
        }
    }
}
