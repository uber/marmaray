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
package com.uber.marmaray.utilities.listener;

import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.util.AbstractSparkTest;
import org.junit.Assert;
import org.junit.Test;

public class TestTimeoutManager extends AbstractSparkTest {

    @Override
    public void setupTest() {
        super.setupTest();
        TimeoutManager.init(new Configuration(), this.jsc.get().sc());
    }

    @Override
    public void teardownTest() {
        super.teardownTest();
        TimeoutManager.close();
    }

    @Test
    public void testTaskStageEventHandling() {
        final TimeoutManager tm = TimeoutManager.getInstance();
        Assert.assertTrue(tm.getLastActiveTime().isEmpty());

        // No update expected when stage finishes but is not getting tracked.
        tm.stageFinished(1);
        Assert.assertTrue(tm.getLastActiveTime().isEmpty());

        // new stage should start getting tracked after stageStart and should be removed from tracking when stage
        // finishes.
        tm.stageStarted(1);
        Assert.assertEquals(1, tm.getLastActiveTime().size());
        // Only stage is started and no tasks are started yet.
        Assert.assertEquals(0, tm.getLastActiveTime().get(1).getRunningTasks().get());
        tm.stageFinished(1);
        Assert.assertEquals(0, tm.getLastActiveTime().size());

        // When task starts it should add stage for tracking.
        tm.taskStarted(1);
        Assert.assertEquals(1, tm.getLastActiveTime().size());
        Assert.assertEquals(1, tm.getLastActiveTime().get(1).getRunningTasks().get());
        // Let us start another task in same stage. It should only increase running tasks.
        tm.taskStarted(1);
        Assert.assertEquals(1, tm.getLastActiveTime().size());
        Assert.assertEquals(2, tm.getLastActiveTime().get(1).getRunningTasks().get());
        // finishing one task should not remove stage from tracking..
        tm.taskFinished(1);
        Assert.assertEquals(1, tm.getLastActiveTime().size());
        Assert.assertEquals(1, tm.getLastActiveTime().get(1).getRunningTasks().get());
        // task finish event may get dropped.. therefore whenever we receive stage finish event we remove stage from
        // tracking.
        tm.stageFinished(1);
        Assert.assertEquals(0, tm.getLastActiveTime().size());

        // Let us start tasks from different stages.. it should add 2 different stage trackers.
        tm.taskStarted(1);
        tm.taskStarted(2);
        Assert.assertEquals(2, tm.getLastActiveTime().size());

    }
}
