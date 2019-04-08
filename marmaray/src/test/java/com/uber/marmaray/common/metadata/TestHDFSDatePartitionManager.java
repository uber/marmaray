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
import com.uber.marmaray.common.configuration.HiveSourceConfiguration;
import com.uber.marmaray.utilities.FSUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestHDFSDatePartitionManager {
    private static final String JOBNAME = "jobFoo";
    private static final String FILE1 = "file1";
    private static final String DATESTR = "datestr";
    private static final String RAW_DATA_PATH = HDFSTestConstants.BASE_RAW_DATA_PATH + File.separator + JOBNAME;
    private FileSystem fs;

    @Before
    public void setupTest() throws IOException {
        this.fs = FSUtils.getFs(new Configuration(), Optional.absent());
    }

    @After
    public void tearDownTest() throws IOException {
        this.fs.delete(new Path(HDFSTestConstants.BASE_METADATA_PATH), true);
        this.fs.delete(new Path(HDFSTestConstants.BASE_RAW_DATA_PATH), true);
        this.fs.close();
    }

    @Test
    public void testGetNextPartitionWithNonexistentCheckpoint() throws IOException {
        final Path basePath = new Path(RAW_DATA_PATH, "datestr=2017-05-01");
        this.fs.mkdirs(basePath);

        final HDFSDatePartitionManager pm = new HDFSDatePartitionManager(JOBNAME,
                RAW_DATA_PATH,
                DATESTR,
                Optional.absent(),
                this.fs);

        final HDFSMetadataManager metadataManager = new HDFSMetadataManager(this.fs,
                new Path(HDFSTestConstants.BASE_METADATA_PATH, JOBNAME).toString(),
                new AtomicBoolean(true));

        final Optional<StringValue> latestCheckpoint = metadataManager.get(MetadataConstants.CHECKPOINT_KEY);
        final Optional<String> partition = pm.getNextPartition(latestCheckpoint);
        Assert.assertTrue(partition.isPresent());
        Assert.assertEquals("datestr=2017-05-01", partition.get());
    }

    @Test
    public void testGetNextPartitionWithStartDateAndNoCheckpoint() throws IOException, ParseException {
        this.fs.mkdirs(new Path(RAW_DATA_PATH, "datestr=2017-05-01"));
        this.fs.mkdirs(new Path(RAW_DATA_PATH, "datestr=2017-06-01"));

        final SimpleDateFormat sdf = new SimpleDateFormat(HiveSourceConfiguration.HIVE_START_DATE_FORMAT);
        final Date startDate = sdf.parse("2017-05-15");

        final HDFSDatePartitionManager pm = new HDFSDatePartitionManager(JOBNAME,
                RAW_DATA_PATH,
                DATESTR,
                Optional.of(startDate),
                this.fs);

        final HDFSMetadataManager metadataManager = new HDFSMetadataManager(this.fs,
                new Path(HDFSTestConstants.BASE_METADATA_PATH, JOBNAME).toString(),
                new AtomicBoolean(true));

        final Optional<StringValue> latestCheckpoint = metadataManager.get(MetadataConstants.CHECKPOINT_KEY);
        final Optional<String> partition = pm.getNextPartition(latestCheckpoint);
        Assert.assertTrue(partition.isPresent());
        Assert.assertEquals("datestr=2017-06-01", partition.get());
    }

    @Test
    public void testGetNextPartitionWitMultipleDatePartitionsAndNoCheckpoint()
        throws IOException {
        this.fs.mkdirs(new Path(RAW_DATA_PATH, "datestr=2017-05-01"));
        this.fs.mkdirs(new Path(RAW_DATA_PATH, "datestr=2017-05-02"));

        final HDFSDatePartitionManager pm = new HDFSDatePartitionManager(JOBNAME,
                RAW_DATA_PATH,
                DATESTR,
                Optional.absent(),
                this.fs);

        final HDFSMetadataManager metadataManager = new HDFSMetadataManager(this.fs,
                new Path(HDFSTestConstants.BASE_METADATA_PATH, JOBNAME).toString(),
                new AtomicBoolean(true));

        final Optional<StringValue> latestCheckpoint = metadataManager.get(MetadataConstants.CHECKPOINT_KEY);
        final Optional<String> partition = pm.getNextPartition(latestCheckpoint);
        Assert.assertTrue(partition.isPresent());
        Assert.assertEquals("datestr=2017-05-01", partition.get());
    }

    @Test
    public void testGetNextPartitionWithMultipleDatePartitionsAndOneCheckpoint()
            throws IOException {
        // Job has multiple data partitions, one is less than checkpoint and the other is larger
        final Path partition1 = new Path(RAW_DATA_PATH, "datestr=2017-05-01");
        final Path partition2 = new Path(RAW_DATA_PATH, "datestr=2017-05-03");

        this.fs.mkdirs(new Path(partition1, FILE1));
        this.fs.mkdirs(new Path(partition2, FILE1));


        final StringValue val1 = new StringValue("datestr=2017-05-02");

        final HDFSDatePartitionManager pm = new HDFSDatePartitionManager(JOBNAME,
                RAW_DATA_PATH,
                DATESTR,
                Optional.absent(),
                this.fs);

        final HDFSMetadataManager metadataManager = new HDFSMetadataManager(this.fs,
                new Path(HDFSTestConstants.BASE_METADATA_PATH, JOBNAME).toString(),
                new AtomicBoolean(true));

        metadataManager.set(MetadataConstants.CHECKPOINT_KEY, val1);
        metadataManager.saveChanges();

        final Optional<StringValue> latestCheckpoint = metadataManager.get(MetadataConstants.CHECKPOINT_KEY);
        final Optional<String> partition = pm.getNextPartition(latestCheckpoint);
        Assert.assertTrue(partition.isPresent());
        Assert.assertEquals("datestr=2017-05-03", partition.get());
    }

    @Test
    public void testGetNextPartitionWithCheckpointLaterThanStartDate()
            throws IOException, ParseException {
        this.fs.mkdirs(new Path(RAW_DATA_PATH, "datestr=2017-05-01"));
        this.fs.mkdirs(new Path(RAW_DATA_PATH, "datestr=2017-06-01"));
        this.fs.mkdirs(new Path(RAW_DATA_PATH, "datestr=2017-07-01"));

        final SimpleDateFormat sdf = new SimpleDateFormat(HiveSourceConfiguration.HIVE_START_DATE_FORMAT);
        final Date startDate = sdf.parse("2017-05-03");

        final StringValue val1 = new StringValue("datestr=2017-06-02");

        final HDFSDatePartitionManager pm = new HDFSDatePartitionManager(JOBNAME,
                RAW_DATA_PATH,
                DATESTR,
                Optional.of(startDate),
                this.fs);

        final HDFSMetadataManager metadataManager = new HDFSMetadataManager(this.fs,
                new Path(HDFSTestConstants.BASE_METADATA_PATH, JOBNAME).toString(),
                new AtomicBoolean(true));

        metadataManager.set(MetadataConstants.CHECKPOINT_KEY, val1);
        metadataManager.saveChanges();

        final Optional<StringValue> latestCheckpoint = metadataManager.get(MetadataConstants.CHECKPOINT_KEY);
        final Optional<String> partition = pm.getNextPartition(latestCheckpoint);
        Assert.assertTrue(partition.isPresent());
        Assert.assertEquals("datestr=2017-07-01", partition.get());
    }

    @Test
    public void testGetNextPartitionWithCheckpointBeforeThanStartDate()
            throws IOException, ParseException {
        this.fs.mkdirs(new Path(RAW_DATA_PATH, "datestr=2017-05-01"));
        this.fs.mkdirs(new Path(RAW_DATA_PATH, "datestr=2017-06-01"));
        this.fs.mkdirs(new Path(RAW_DATA_PATH, "datestr=2017-07-01"));

        final SimpleDateFormat sdf = new SimpleDateFormat(HiveSourceConfiguration.HIVE_START_DATE_FORMAT);
        final Date startDate = sdf.parse("2017-06-01");

        final StringValue val1 = new StringValue("datestr=2017-05-02");

        final HDFSDatePartitionManager pm = new HDFSDatePartitionManager(JOBNAME,
                RAW_DATA_PATH,
                DATESTR,
                Optional.of(startDate),
                this.fs);

        final HDFSMetadataManager metadataManager = new HDFSMetadataManager(this.fs,
                new Path(HDFSTestConstants.BASE_METADATA_PATH, JOBNAME).toString(),
                new AtomicBoolean(true));


        metadataManager.set(MetadataConstants.CHECKPOINT_KEY, val1);
        metadataManager.saveChanges();

        final Optional<StringValue> latestCheckpoint = metadataManager.get(MetadataConstants.CHECKPOINT_KEY);
        final Optional<String> partition = pm.getNextPartition(latestCheckpoint);
        Assert.assertTrue(partition.isPresent());
        Assert.assertEquals("datestr=2017-07-01", partition.get());
    }


    @Test
    public void testGetNextPartitionWithSmallerExistentCheckpoint() throws IOException, InterruptedException {
        final StringValue val1 = new StringValue("datestr=2017-05-01");

        final Path partition1 = new Path(RAW_DATA_PATH, "datestr=2017-05-02");
        this.fs.mkdirs(new Path(partition1, FILE1));

        final HDFSDatePartitionManager pm = new HDFSDatePartitionManager(JOBNAME,
                RAW_DATA_PATH,
                DATESTR,
                Optional.absent(),
                this.fs);

        final HDFSMetadataManager metadataManager = new HDFSMetadataManager(this.fs,
                new Path(HDFSTestConstants.BASE_METADATA_PATH, JOBNAME).toString(),
                new AtomicBoolean(true));

        metadataManager.set(MetadataConstants.CHECKPOINT_KEY, val1);
        metadataManager.saveChanges();

        final Optional<StringValue> latestCheckpoint = metadataManager.get(MetadataConstants.CHECKPOINT_KEY);
        final Optional<String> partition = pm.getNextPartition(latestCheckpoint);
        Assert.assertTrue(partition.isPresent());
        Assert.assertEquals("datestr=2017-05-02", partition.get());
    }

    @Test
    public void testGetNextPartitionWithLargerExistentCheckpoint() throws IOException, InterruptedException {
        // In this case the checkpoint is larger than the data partition so there is no "next" partition
        final StringValue val1 = new StringValue("datestr=2017-05-02");

        final Path partition1 = new Path(RAW_DATA_PATH, "datestr=2017-05-01");
        this.fs.mkdirs(new Path(partition1, FILE1));

        final HDFSDatePartitionManager pm = new HDFSDatePartitionManager(JOBNAME,
                RAW_DATA_PATH,
                DATESTR,
                Optional.absent(),
                this.fs);

        final HDFSMetadataManager metadataManager = new HDFSMetadataManager(this.fs,
                new Path(HDFSTestConstants.BASE_METADATA_PATH, JOBNAME).toString(),
                new AtomicBoolean(true));

        metadataManager.set(MetadataConstants.CHECKPOINT_KEY, val1);
        metadataManager.saveChanges();

        final Optional<StringValue> latestCheckpoint = metadataManager.get(MetadataConstants.CHECKPOINT_KEY);
        final Optional<String> partition = pm.getNextPartition(latestCheckpoint);
        Assert.assertFalse(partition.isPresent());
    }
}
