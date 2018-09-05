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
package com.uber.marmaray.common.sinks;

import com.uber.marmaray.common.metadata.IMetadataManager;
import com.uber.marmaray.common.metadata.MemoryMetadataManager;
import com.uber.marmaray.common.sinks.SinkStatManager.SinkStat;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

@Slf4j
public class TestSinkStatManager {

    @Test
    public void testSerDser() {
        final String tableName = "testTable";
        final IMetadataManager metadataManager = new MemoryMetadataManager();
        final SinkStatManager sinkStatManager1 = new SinkStatManager(tableName, metadataManager);

        // Initially nothing will be found; it should not crash.
        sinkStatManager1.init();
        Assert.assertEquals(0, sinkStatManager1.getAvgRecordSize());
        Assert.assertFalse(sinkStatManager1.isStatHistoryAvailable());

        final int avgRecordSize1 = 30;
        sinkStatManager1.getCurrentStat().put(SinkStat.AVG_RECORD_SIZE, Integer.toString(avgRecordSize1));
        // nothing is saved to metadata manager before persist.
        Assert.assertEquals(0, metadataManager.getAllKeys().size());
        sinkStatManager1.persist();
        Assert.assertEquals(1, metadataManager.getAllKeys().size());

        final SinkStatManager sinkStatManager2 = new SinkStatManager(tableName, metadataManager);
        sinkStatManager2.init();
        Assert.assertEquals(avgRecordSize1, sinkStatManager2.getAvgRecordSize());
        final int avgRecordSize2 = 20;
        sinkStatManager2.getCurrentStat().put(SinkStat.AVG_RECORD_SIZE, Integer.toString(avgRecordSize2));

        sinkStatManager2.persist();

        final SinkStatManager sinkStatManager3 = new SinkStatManager(tableName, metadataManager);
        sinkStatManager3.init();
        Assert.assertEquals((avgRecordSize1 + avgRecordSize2) / 2, sinkStatManager3.getAvgRecordSize());
    }

    @Test
    public void testMaxStatHistory() {
        final String tableName = "testTable";
        final IMetadataManager metadataManager = new MemoryMetadataManager();
        final SinkStatManager sinkStatManager1 = new SinkStatManager(tableName, metadataManager);
        sinkStatManager1.init();
        final int initialValue = SinkStatManager.MAX_HISTORY_SIZE * 2;
        sinkStatManager1.getCurrentStat().put(SinkStat.AVG_RECORD_SIZE, Integer.toString(initialValue));
        sinkStatManager1.persist();
        final int targetValue = 1;
        for (int i = 0; i < SinkStatManager.MAX_HISTORY_SIZE; i++) {
            final SinkStatManager tempSinkStatManager = new SinkStatManager(tableName, metadataManager);
            tempSinkStatManager.init();
            tempSinkStatManager.getCurrentStat().put(SinkStat.AVG_RECORD_SIZE, Integer.toString(targetValue));
            Assert.assertNotEquals(targetValue, tempSinkStatManager.getAvgRecordSize());
            tempSinkStatManager.persist();
        }

        // After SinkStatManager.MAX_HISTORY_SIZE runs very first stat should get dropped.
        final SinkStatManager sinkStatManager2 = new SinkStatManager(tableName, metadataManager);
        sinkStatManager2.init();
        Assert.assertEquals(targetValue, sinkStatManager2.getAvgRecordSize());
    }
}
