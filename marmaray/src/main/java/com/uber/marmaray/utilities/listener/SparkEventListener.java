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

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.scheduler.SparkListenerTaskStart;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.scheduler.TaskInfo;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SparkEventListener extends SparkListener {

    @Override
    public void onStageSubmitted(final SparkListenerStageSubmitted stageSubmitted) {
        SparkJobTracker.recordStageInfo(stageSubmitted.stageInfo(), stageSubmitted.properties());
    }

    @Override
    public void onStageCompleted(final SparkListenerStageCompleted stageCompleted) {
        TimeoutManager.getInstance().setLastEventTime(stageCompleted.stageInfo().stageId());
        final StageInfo stageInfo = stageCompleted.stageInfo();
        if (stageInfo.completionTime().isDefined() && stageInfo.submissionTime().isDefined()) {
            SparkJobTracker.recordStageTime(stageInfo,
                    (long) stageInfo.completionTime().get() - (long) stageInfo.submissionTime().get());
        } else {
            log.error("Stage completed without submission or completion time. Stage {}: {}",
                    stageInfo.stageId(), stageInfo.name());
        }
        SparkJobTracker.removeStageInfo(stageInfo);
    }

    @Override
    public void onTaskEnd(final SparkListenerTaskEnd taskEnd) {
        final TaskInfo taskInfo = taskEnd.taskInfo();
        TimeoutManager.getInstance().setLastEventTime(taskEnd.stageId());
        SparkJobTracker.recordTaskTime(taskEnd.stageId(), taskInfo.finishTime() - taskInfo.launchTime());
    }

    @Override
    public void onTaskStart(final SparkListenerTaskStart taskStart) {
        TimeoutManager.getInstance().setLastEventTime(taskStart.stageId());
    }

    @Override
    public void onApplicationEnd(final SparkListenerApplicationEnd applicationEnd) {
        SparkJobTracker.logResult();
    }
}
