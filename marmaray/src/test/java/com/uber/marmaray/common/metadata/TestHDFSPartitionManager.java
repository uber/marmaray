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
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import lombok.NonNull;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestHDFSPartitionManager {

    private static final String JOBNAME = "jobFoo";
    private static final String PARTITION0 = "partition0";
    private static final String PARTITION1 = "partition1";
    private static final String PARTITION2 = "partition2";
    private static final String PARTITION3 = "partition3";
    private static final String FILE1 = "file1";
    private static final String METADATA_PATH = HDFSTestConstants.BASE_METADATA_PATH + File.separator + JOBNAME;
    private static final String RAW_DATA_PATH = HDFSTestConstants.BASE_RAW_DATA_PATH + File.separator + JOBNAME;
    private FileSystem fileSystem;

    @Before
    public void setupTest() throws IOException {
        this.fileSystem = FSUtils.getFs(new Configuration());
    }

    @After
    public void tearDownTest() throws IOException {
        this.fileSystem.delete(new Path(METADATA_PATH), true);
        this.fileSystem.delete(new Path(HDFSTestConstants.BASE_RAW_DATA_PATH), true);

        if (this.fileSystem != null) {
            this.fileSystem.close();
        }
    }

    @Test
    public void testGetNextPartitionWithNonExistentCheckpoint() throws InterruptedException, IOException {
        final Path partitionPath = new Path(RAW_DATA_PATH, PARTITION1);
        final Path filePath = new Path(partitionPath, FILE1);

        this.fileSystem.create(filePath);

        final HDFSPartitionManager pm = new HDFSPartitionManager(JOBNAME,
                HDFSTestConstants.BASE_METADATA_PATH,
                RAW_DATA_PATH,
                this.fileSystem);

        final HDFSMetadataManager metadataManager = new HDFSMetadataManager(this.fileSystem,
                new Path(HDFSTestConstants.BASE_METADATA_PATH, JOBNAME).toString(),
                new AtomicBoolean(true));

        final Optional<String> partition = pm.getNextPartition(getLatestCheckpoint(metadataManager));
        Assert.assertTrue(partition.isPresent());
        Assert.assertEquals(PARTITION1, partition.get());
    }

    @Test
    public void testGetNextPartitionWithOnlyTempFileCheckpoints() throws InterruptedException, IOException {
        final Path partitionPath = new Path(RAW_DATA_PATH, PARTITION1);
        final Path filePath = new Path(partitionPath, FILE1);

        this.fileSystem.create(filePath);

        final HDFSPartitionManager pm = new HDFSPartitionManager(JOBNAME,
                HDFSTestConstants.BASE_METADATA_PATH,
                RAW_DATA_PATH,
                this.fileSystem);

        final HDFSMetadataManager metadataManager = new HDFSMetadataManager(this.fileSystem,
                new Path(HDFSTestConstants.BASE_METADATA_PATH, JOBNAME).toString(),
                new AtomicBoolean(true));

        // if this metadata was saved successfully we would say there's no partition to process
        // but this will be in a temp file so it will be ignored
        metadataManager.set(MetadataConstants.CHECKPOINT_KEY, new StringValue(PARTITION2));
        metadataManager.saveChanges();

        final Optional<FileStatus> fs = metadataManager.getLatestMetadataFile();
        Assert.assertTrue(fs.isPresent());

        // move the metadata file back to a temp location
        this.fileSystem.rename(fs.get().getPath(),
                new Path(fs.get().getPath().toString() + MetadataConstants.TEMP_FILE_EXTENSION));

        final Optional<String> partition = pm.getNextPartition(getLatestCheckpoint(metadataManager));
        Assert.assertTrue(partition.isPresent());
        Assert.assertEquals(PARTITION1, partition.get());
    }

    @Test
    public void testGetNextPartitionSinglePartition() throws IOException, InterruptedException {
        final Path partitionPath = new Path(RAW_DATA_PATH, PARTITION2);
        final Path filePath = new Path(partitionPath, FILE1);
        this.fileSystem.create(filePath);

        final StringValue val1 = new StringValue(PARTITION1);
        final HDFSPartitionManager pm = new HDFSPartitionManager(JOBNAME,
                HDFSTestConstants.BASE_METADATA_PATH,
                RAW_DATA_PATH,
                this.fileSystem);

        final HDFSMetadataManager metadataManager = new HDFSMetadataManager(this.fileSystem,
                new Path(HDFSTestConstants.BASE_METADATA_PATH, JOBNAME).toString(),
                new AtomicBoolean(true));

        metadataManager.set(MetadataConstants.CHECKPOINT_KEY, val1);
        metadataManager.saveChanges();

        final Optional<StringValue> latestCheckpoint = getLatestCheckpoint(metadataManager);
        Assert.assertTrue(pm.getNextPartition(latestCheckpoint).isPresent());
        Assert.assertTrue(pm.getNextPartition(latestCheckpoint).get().equals(PARTITION2));
    }

    @Test
    public void testGetNextPartitionMultipleDataPartitions() throws IOException, InterruptedException {
        final StringValue val1 = new StringValue(PARTITION1);

        final Path partition2Path = new Path(RAW_DATA_PATH, PARTITION2);
        final Path partition3Path = new Path(RAW_DATA_PATH, PARTITION3);
        this.fileSystem.create(new Path(partition2Path, FILE1));
        this.fileSystem.create(new Path(partition3Path, FILE1));

        final HDFSPartitionManager pm = new HDFSPartitionManager(JOBNAME,
                HDFSTestConstants.BASE_METADATA_PATH,
                RAW_DATA_PATH,
                this.fileSystem);

        final HDFSMetadataManager metadataManager = new HDFSMetadataManager(this.fileSystem,
                new Path(HDFSTestConstants.BASE_METADATA_PATH, JOBNAME).toString(),
                new AtomicBoolean(true));

        metadataManager.set(MetadataConstants.CHECKPOINT_KEY, val1);
        metadataManager.saveChanges();

        final Optional<StringValue> latestCheckpoint = getLatestCheckpoint(metadataManager);
        Assert.assertTrue(pm.getNextPartition(latestCheckpoint).isPresent());
        Assert.assertTrue(pm.getNextPartition(latestCheckpoint).get().equals(PARTITION2));
    }

    @Test
    public void testGetLastCheckpointMultipleCheckpoints() throws IOException, InterruptedException {
        final Path partition1Path = new Path(RAW_DATA_PATH, PARTITION1);
        final Path partition2Path = new Path(RAW_DATA_PATH, PARTITION2);
        this.fileSystem.create(new Path(partition1Path, FILE1));
        this.fileSystem.create(new Path(partition2Path, FILE1));

        final HDFSPartitionManager pm = new HDFSPartitionManager(JOBNAME,
                HDFSTestConstants.BASE_METADATA_PATH,
                RAW_DATA_PATH,
                this.fileSystem);

        final HDFSMetadataManager metadataManager = new HDFSMetadataManager(this.fileSystem,
                new Path(HDFSTestConstants.BASE_METADATA_PATH, JOBNAME).toString(),
                new AtomicBoolean(true));

        final Optional<StringValue> latestCheckpoint = getLatestCheckpoint(metadataManager);
        Assert.assertTrue(pm.getNextPartition(latestCheckpoint).isPresent());
        Assert.assertEquals(pm.getNextPartition(latestCheckpoint).get(), PARTITION1);

        metadataManager.set(MetadataConstants.CHECKPOINT_KEY, new StringValue(PARTITION1));
        metadataManager.saveChanges();

        final HDFSPartitionManager pm2 = new HDFSPartitionManager(JOBNAME,
                HDFSTestConstants.BASE_METADATA_PATH,
                RAW_DATA_PATH,
                this.fileSystem);

        final HDFSMetadataManager metadataManager2 = new HDFSMetadataManager(this.fileSystem,
                new Path(HDFSTestConstants.BASE_METADATA_PATH, JOBNAME).toString(),
                new AtomicBoolean(true));

        final Optional<StringValue> latestCheckpoint2 = metadataManager2.get(MetadataConstants.CHECKPOINT_KEY);
        Assert.assertTrue(latestCheckpoint2.isPresent());
        Assert.assertEquals(PARTITION1, latestCheckpoint2.get().getValue());
        Assert.assertTrue(pm2.getNextPartition(latestCheckpoint2).isPresent());
        Assert.assertEquals(pm2.getNextPartition(latestCheckpoint2).get(), PARTITION2);

        metadataManager2.set(MetadataConstants.CHECKPOINT_KEY, new StringValue(PARTITION2));
        metadataManager2.saveChanges();


        final HDFSPartitionManager pm3 = new HDFSPartitionManager(JOBNAME,
                HDFSTestConstants.BASE_METADATA_PATH,
                RAW_DATA_PATH,
                this.fileSystem);


        final HDFSMetadataManager metadataManager3 = new HDFSMetadataManager(this.fileSystem,
                new Path(HDFSTestConstants.BASE_METADATA_PATH, JOBNAME).toString(),
                new AtomicBoolean(true));

        final Optional<StringValue> latestCheckpoint3 = metadataManager3.get(MetadataConstants.CHECKPOINT_KEY);
        Assert.assertTrue(latestCheckpoint3.isPresent());
        Assert.assertEquals(PARTITION2, latestCheckpoint3.get().getValue());
        Assert.assertFalse(pm3.getNextPartition(latestCheckpoint3).isPresent());
    }

    @Test
    public void testGetNextPartitionCheckpointIsLargerThanPartition() throws InterruptedException, IOException {
        final Path partition2Path = new Path(RAW_DATA_PATH, PARTITION2);
        this.fileSystem.mkdirs(new Path(partition2Path, FILE1));

        final StringValue val1 = new StringValue(PARTITION2);

        final HDFSPartitionManager pm = new HDFSPartitionManager(JOBNAME,
                HDFSTestConstants.BASE_METADATA_PATH,
                RAW_DATA_PATH,
                this.fileSystem);

        final HDFSMetadataManager metadataManager = new HDFSMetadataManager(this.fileSystem,
                new Path(HDFSTestConstants.BASE_METADATA_PATH, JOBNAME).toString(),
                new AtomicBoolean(true));

        metadataManager.set(MetadataConstants.CHECKPOINT_KEY, val1);
        metadataManager.saveChanges();

        final Path partition1Path = new Path(RAW_DATA_PATH, PARTITION1);
        this.fileSystem.mkdirs(new Path(partition1Path, FILE1));

        // Checkpoint value is greater than the partitions in the data folder so nothing new to process
        Assert.assertFalse(pm.getNextPartition(getLatestCheckpoint(metadataManager)).isPresent());
    }

    @Test
    public void testGetExistingPartitions() throws IOException {
        final Path partition0Path = new Path(RAW_DATA_PATH, PARTITION0);
        final Path partition1Path = new Path(RAW_DATA_PATH, PARTITION1);
        final Path partition2Path = new Path(RAW_DATA_PATH, PARTITION2);
        this.fileSystem.create(new Path(partition0Path, FILE1));
        this.fileSystem.create(new Path(partition1Path, FILE1));
        this.fileSystem.create(new Path(partition2Path, FILE1));

        final HDFSPartitionManager pm = new HDFSPartitionManager(JOBNAME,
                HDFSTestConstants.BASE_METADATA_PATH,
                RAW_DATA_PATH,
                this.fileSystem);

        Assert.assertEquals(3, pm.getExistingPartitions().size());

        for (int i = 0; i < pm.getExistingPartitions().size(); i++) {
            Assert.assertEquals("partition" + i, pm.getExistingPartitions().get(i));
        }
    }

    @Test
    public void testGetExistingPartitionsOnlyFilesExist() throws IOException {
        final Path partition0File = new Path(RAW_DATA_PATH, PARTITION0);
        this.fileSystem.create(partition0File);

        final HDFSPartitionManager pm = new HDFSPartitionManager(JOBNAME,
                HDFSTestConstants.BASE_METADATA_PATH,
                RAW_DATA_PATH,
                this.fileSystem);

        final HDFSMetadataManager metadataManager = new HDFSMetadataManager(this.fileSystem,
                new Path(HDFSTestConstants.BASE_METADATA_PATH, JOBNAME).toString(),
                new AtomicBoolean(true));

        Assert.assertEquals(1, pm.getExistingPartitions().size());
        Assert.assertEquals(RAW_DATA_PATH, pm.getNextPartition(getLatestCheckpoint(metadataManager)).get());
    }


    private Optional<StringValue> getLatestCheckpoint(@NonNull HDFSMetadataManager metadataManager) throws IOException {
        final Map<String, StringValue> metadataMap = metadataManager.loadMetadata();

        return metadataMap.containsKey(MetadataConstants.CHECKPOINT_KEY)
                ? Optional.of(metadataMap.get(MetadataConstants.CHECKPOINT_KEY))
                : Optional.absent();
    }
}

