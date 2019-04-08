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

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestHDFSMetadataManager {

    private static final String JOB_NAME = "jobName";
    private FileSystem fileSystem;
    private HDFSMetadataManager metadataManager;

    @Before
    public void setupTest() throws IOException {
        this.fileSystem = FSUtils.getFs(new Configuration(), Optional.absent());
        final AtomicBoolean condition = new AtomicBoolean(true);
        final String metadataPath = new Path(HDFSTestConstants.BASE_METADATA_PATH, JOB_NAME).toString();
        this.metadataManager = new HDFSMetadataManager(this.fileSystem, metadataPath, condition);
    }

    @After
    public void tearDownTest() throws IOException {
        this.fileSystem.delete(new Path(HDFSTestConstants.BASE_METADATA_PATH), true);

        if (this.fileSystem != null) {
            this.fileSystem.close();
        }
    }

    @Test
    public void testHDFSReadWriteSingleMetadataFile() throws IOException {
        // Test in memory
        final StringValue val = new StringValue("testVal");
        this.metadataManager.set(MetadataConstants.CHECKPOINT_KEY, val);
        final Optional<StringValue> readValue = this.metadataManager.get(MetadataConstants.CHECKPOINT_KEY);
        Assert.assertTrue(readValue.isPresent());
        Assert.assertTrue(readValue.get().getValue().equals("testVal"));

        this.metadataManager.set("foo", new StringValue("bar"));

        // Serialize the metadata map to a file
        this.metadataManager.saveChanges();
        final Optional<FileStatus> fs = this.metadataManager.getLatestMetadataFile();
        Assert.assertTrue(fs.isPresent());
        // Deserialize the metadata map and check contents are the same
        final Map<String, StringValue> loadedMap = this.metadataManager.loadMetadata(fs.get().getPath());
        validateDeserializedMapEqualsInMemoryMap(loadedMap);
    }

    @Test
    public void testHDFSOverwriteCheckpointValue() throws IOException, InterruptedException {
        final StringValue val1 = new StringValue("testVal");
        this.metadataManager.set(MetadataConstants.CHECKPOINT_KEY, val1);

        final StringValue val2 = new StringValue("testVal2");
        this.metadataManager.set(MetadataConstants.CHECKPOINT_KEY, val2);

        final Optional<StringValue> readValue = this.metadataManager.get(MetadataConstants.CHECKPOINT_KEY);
        Assert.assertTrue(readValue.isPresent());
        Assert.assertTrue(readValue.get().getValue().equals("testVal2"));

        this.metadataManager.saveChanges();

        final Optional<FileStatus> fs = this.metadataManager.getLatestMetadataFile();
        Assert.assertTrue(fs.isPresent());
        final Map<String, StringValue> loadedMap = this.metadataManager.loadMetadata(fs.get().getPath());
        validateDeserializedMapEqualsInMemoryMap(loadedMap);
    }

    @Test
    public void testDeletionIsPropagated() throws Exception {
        final StringValue val1 = new StringValue("testVal");
        this.metadataManager.set(MetadataConstants.CHECKPOINT_KEY, val1);

        this.metadataManager.saveChanges();
        Optional<FileStatus> fs = this.metadataManager.getLatestMetadataFile();
        Assert.assertTrue(fs.isPresent());
        Map<String, StringValue> loadedMap = this.metadataManager.loadMetadata(fs.get().getPath());
        validateDeserializedMapEqualsInMemoryMap(loadedMap);

        // reload the configuration
        setupTest();
        Assert.assertTrue(this.metadataManager.get(MetadataConstants.CHECKPOINT_KEY).isPresent());

        this.metadataManager.remove(MetadataConstants.CHECKPOINT_KEY);
        Assert.assertFalse(this.metadataManager.get(MetadataConstants.CHECKPOINT_KEY).isPresent());
        this.metadataManager.saveChanges();
        fs = this.metadataManager.getLatestMetadataFile();
        Assert.assertFalse(this.metadataManager.get(MetadataConstants.CHECKPOINT_KEY).isPresent());
        loadedMap = this.metadataManager.loadMetadata(fs.get().getPath());
        validateDeserializedMapEqualsInMemoryMap(loadedMap);
    }

    private void validateDeserializedMapEqualsInMemoryMap(final Map<String, StringValue> deserializedMap) {
        for (Map.Entry<String, StringValue> entry : deserializedMap.entrySet()) {
            final Optional<StringValue> valueInMemory = this.metadataManager.get(entry.getKey());
            Assert.assertTrue(valueInMemory.isPresent());
            Assert.assertEquals(valueInMemory.get().getValue(), entry.getValue().getValue());
        }

        Assert.assertEquals(this.metadataManager.getAllKeys(), deserializedMap.keySet());
    }

    // T918085 - Todo: Add test to verify metadata files are culled
}
