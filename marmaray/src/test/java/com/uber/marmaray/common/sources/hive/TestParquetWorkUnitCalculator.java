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
package com.uber.marmaray.common.sources.hive;

import com.google.common.base.Optional;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.metadata.HDFSPartitionManager;
import com.uber.marmaray.common.metadata.MetadataConstants;
import com.uber.marmaray.common.metadata.StringValue;
import com.uber.marmaray.common.sources.IWorkUnitCalculator;
import com.uber.marmaray.common.util.FileTestUtil;
import com.uber.marmaray.utilities.FSUtils;
import com.uber.marmaray.utilities.StringTypes;
import lombok.NonNull;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.hibernate.validator.constraints.NotEmpty;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class TestParquetWorkUnitCalculator {

    private static final String JOB_NAME = "jobFoo";
    private static final String PARTITION_1 = "partition1";
    private static final String PARTITION_2 = "partition2";
    private static final String PARTITION_3 = "partition3";
    private FileSystem fileSystem;
    private HDFSPartitionManager partitionManager;
    private String dataPath;
    private String metadataPath;


    @Before
    public void setupTest() throws IOException {
        this.fileSystem = FSUtils.getFs(new Configuration());
        this.dataPath = FileTestUtil.getTempFolder();
        this.metadataPath = FileTestUtil.getTempFolder();
    }

    @After
    public void tearDownTest() throws IOException {
        if (this.fileSystem != null) {
            this.fileSystem.close();
        }
    }

    @Test
    public void testComputeWorkUnitsWithNoPrexistentCheckpointsMultiplePartitions() throws IOException {
        // No prexisting checkpoints for the workunit calculator
        this.fileSystem.mkdirs(new Path(this.dataPath, PARTITION_2));
        this.fileSystem.mkdirs(new Path(this.dataPath, PARTITION_3));

        this.partitionManager = new HDFSPartitionManager(JOB_NAME,
                this.metadataPath,
                this.dataPath,
                this.fileSystem);

        Assert.assertFalse(this.partitionManager.isSinglePartition());
        Assert.assertFalse(this.partitionManager.getLatestCheckpoint().isPresent());
        virtuallyProcessPartition(this.partitionManager, Optional.absent(), PARTITION_2);

        final HDFSPartitionManager partitionManager2 = new HDFSPartitionManager(JOB_NAME,
                this.metadataPath,
                this.dataPath,
                this.fileSystem);
        Assert.assertTrue(partitionManager2.getLatestCheckpoint().isPresent());
        virtuallyProcessPartition(partitionManager2, Optional.of(new StringValue(PARTITION_2)), PARTITION_3);
    }

    @Test
    public void testComputeWorkUnitsWithNoPrexistentCheckpointsSinglePartition() throws IOException {
        // No prexisting checkpoints for the workunit calculator
        final String dataFileName = "data.parquet";
        this.fileSystem.create(new Path(this.dataPath, dataFileName));

        this.partitionManager = new HDFSPartitionManager(JOB_NAME,
                this.metadataPath,
                this.dataPath,
                this.fileSystem);

        Assert.assertTrue(this.partitionManager.isSinglePartition());
        Assert.assertFalse(this.partitionManager.getLatestCheckpoint().isPresent());

        virtuallyProcessPartition(this.partitionManager, Optional.absent(), this.dataPath );

        // A checkpoint now exists.  Now virtually reprocess that single partition explicitly via data path
        // by initializing and saving run states
        final HDFSPartitionManager pm2 = new HDFSPartitionManager(JOB_NAME,
                this.metadataPath,
                this.dataPath,
                this.fileSystem);
        Assert.assertTrue(pm2.getLatestCheckpoint().isPresent());
        Assert.assertEquals(this.dataPath, pm2.getLatestCheckpoint().get().getValue());

        final ParquetWorkUnitCalculator calc = new ParquetWorkUnitCalculator();
        calc.initPreviousRunState(pm2);
        Assert.assertTrue(calc.getNextPartition().isPresent());
        Assert.assertEquals(this.dataPath, calc.getNextPartition().get());

        // explicitly remove the old checkpoint so we see it is set correctly to latest checkpoint when
        // saving next run state
        pm2.set(MetadataConstants.CHECKPOINT_KEY, new StringValue(StringTypes.EMPTY));
        calc.initPreviousRunState(pm2);
        final IWorkUnitCalculator.IWorkUnitCalculatorResult<String, HiveRunState> iresult = calc.computeWorkUnits();
        calc.saveNextRunState(pm2, iresult.getNextRunState());
        Assert.assertEquals(this.dataPath, pm2.get(MetadataConstants.CHECKPOINT_KEY).get().getValue());
    }

    @Test
    public void testComputeWorkUnitsWithExistentCheckpoint() throws IOException {

        this.fileSystem.mkdirs(new Path(this.dataPath, PARTITION_1));
        this.fileSystem.mkdirs(new Path(this.dataPath, PARTITION_3));

        this.partitionManager = new HDFSPartitionManager(JOB_NAME,
                this.metadataPath,
                this.dataPath,
                this.fileSystem);

        // partition 1 is in effect already processed since the checkpoint is larger
        final StringValue val1 = new StringValue(PARTITION_2);
        this.partitionManager.set(MetadataConstants.CHECKPOINT_KEY, val1);
        this.partitionManager.saveChanges();

        final ParquetWorkUnitCalculator calculator = new ParquetWorkUnitCalculator();
        calculator.initPreviousRunState(this.partitionManager);
        final IWorkUnitCalculator.IWorkUnitCalculatorResult iresult = calculator.computeWorkUnits();
        Assert.assertTrue(iresult instanceof ParquetWorkUnitCalculatorResult);
        final ParquetWorkUnitCalculatorResult result =
                (ParquetWorkUnitCalculatorResult) iresult;
        final List<String> workUnits = result.getWorkUnits();
        Assert.assertEquals(1, workUnits.size());
        Assert.assertEquals(PARTITION_3, workUnits.get(0));
        Assert.assertTrue(result.getNextRunState().getPartition().isPresent());
        Assert.assertEquals(PARTITION_3, result.getNextRunState().getPartition().get());
    }

    private void virtuallyProcessPartition(@NonNull final HDFSPartitionManager partitionManager,
                                           @NotEmpty final Optional<StringValue> expectedLatestCheckpoint,
                                           @NotEmpty final String expectedNextPartition) {
        Assert.assertEquals(expectedLatestCheckpoint, partitionManager.getLatestCheckpoint());

        final ParquetWorkUnitCalculator calculator = new ParquetWorkUnitCalculator();
        calculator.initPreviousRunState(partitionManager);
        final ParquetWorkUnitCalculatorResult result = calculator.computeWorkUnits();
        final List<String> workUnits = result.getWorkUnits();
        Assert.assertEquals(1, workUnits.size());
        Assert.assertEquals(expectedNextPartition, workUnits.get(0));
        Assert.assertTrue(result.getNextRunState().getPartition().isPresent());
        Assert.assertEquals(expectedNextPartition, result.getNextRunState().getPartition().get());
        calculator.saveNextRunState(partitionManager, result.getNextRunState());
        Assert.assertEquals(expectedNextPartition, partitionManager.get(MetadataConstants.CHECKPOINT_KEY).get().getValue());
        partitionManager.saveChanges();
    }
}
