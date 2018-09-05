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
package com.uber.marmaray.common.actions;

import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.job.ThreadPoolService;
import com.uber.marmaray.common.metrics.LongMetric;
import com.uber.marmaray.common.reporters.IReporter;
import com.uber.marmaray.common.reporters.Reporters;
import com.uber.marmaray.common.util.MultiThreadTestCoordinator;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.uber.marmaray.common.metrics.DataFeedMetricNames.RESULT_FAILURE;
import static com.uber.marmaray.common.metrics.DataFeedMetricNames.RESULT_SUCCESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@Slf4j
public class TestJobDagActions {

    private static final int NUM_THREADS = 4;
    private static final int NUM_JOB_DAG_THREADS = 2;

    @BeforeClass
    public static void setUp() {
        final Configuration conf = new Configuration();
        conf.setProperty(ThreadPoolService.NUM_THREADS, Integer.toString(NUM_THREADS));
        conf.setProperty(ThreadPoolService.JOB_DAG_THREADS, Integer.toString(NUM_JOB_DAG_THREADS));
        try {
            ThreadPoolService.init(conf);
        } catch (JobRuntimeException e) {
            // thread pool service already initialized
        }
    }

    @AfterClass
    public static void shutDown() {
        // make sure that the thread pool service has stopped
        ThreadPoolService.shutdown(true);
    }

    @Test
    public void testExecuteHandlesException() {
        final JobDagActions jobDagActions = new JobDagActions(new Reporters());
        final List<Boolean> output = new ArrayList<>(2);
        jobDagActions.addAction(new TestSuccessfulJobDagAction(output));
        jobDagActions.addAction(new TestFailedJobDagAction());
        jobDagActions.addAction(new TestSuccessfulJobDagAction(output));
        final AtomicBoolean isSuccess = new AtomicBoolean(true);
        isSuccess.set(jobDagActions.execute(true));

        assertEquals(false, isSuccess.get());
        assertEquals(2, output.size());
        assertEquals(output.get(0), true);
        assertEquals(output.get(1), true);
    }

    @Test
    public void testActionsRunInParallel() {
        final MultiThreadTestCoordinator coordinator = new MultiThreadTestCoordinator();
        final JobDagActions jobDagActions = new JobDagActions(new Reporters());
        jobDagActions.addAction(new TestParallelJobAction2(coordinator));
        jobDagActions.addAction(new TestParallelJobAction1(coordinator));
        jobDagActions.execute(true);
    }

    @Test
    public void testWaitForActionsToFinish() {
        final JobDagActions jobDagActions = new JobDagActions(new Reporters());
        final AtomicBoolean success = new AtomicBoolean(false);
        final IJobDagAction act = new IJobDagAction() {
            public boolean execute(final boolean dagSuccess) {
                try {
                    Thread.sleep(10000);
                    success.set(true);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
                return success.get();
            }

            public int getTimeoutSeconds() {
                return 20;
            }
        };

        jobDagActions.addAction(act);
        jobDagActions.execute(true);
        assertTrue(success.get());
    }

    @Test
    public void testTimeoutForLongRunningActions() {
        final JobDagActions jobDagActions = new JobDagActions(new Reporters());
        final MultiThreadTestCoordinator coordinator = new MultiThreadTestCoordinator();
        final AtomicBoolean success = new AtomicBoolean(true);
        jobDagActions.addAction(new TestParallelJobActionFailure(coordinator));
        success.set(jobDagActions.execute(true));
        assertTrue(!success.get());
    }

    @Test
    public void testJobDagMetricsReporting() {
        verifyJobMetricResult(new TestSuccessfulJobDagAction(new LinkedList<>()), true, RESULT_SUCCESS, false);
        verifyJobMetricResult(new TestSuccessfulJobDagAction(new LinkedList<>()), false, RESULT_FAILURE, false);
        verifyJobMetricResult(new TestFailedJobDagAction(), true, RESULT_FAILURE, true);
    }

    private void verifyJobMetricResult(@NonNull final IJobDagAction jobDagAction, final boolean dagSuccess,
        final long expectedMetricValue, final boolean isFailureExpected) {
        final Reporters reporters = new Reporters();
        final IReporter mockReporter = spy(new TestReporter());
        reporters.addReporter(mockReporter);
        final JobDagActions jobDagActions = new JobDagActions(reporters);
        jobDagActions.addAction(jobDagAction);
        Assert.assertEquals(!isFailureExpected, jobDagActions.execute(dagSuccess));
        final ArgumentCaptor<LongMetric> metricCapture = ArgumentCaptor.forClass(LongMetric.class);
        verify(mockReporter, times(2)).gauge(metricCapture.capture());
        Assert.assertEquals(expectedMetricValue, metricCapture.getValue().getMetricValue().longValue());
    }

    private class TestReporter implements IReporter<LongMetric> {

        @Override
        public void gauge(final LongMetric m) {
        }

        @Override
        public void finish() {

        }
    }

    @AllArgsConstructor
    private class TestSuccessfulJobDagAction implements IJobDagAction {
        @NonNull
        private final List<Boolean> output;

        public boolean execute(final boolean success) {
            output.add(success);
            return success;
        }

        @Override
        public int getTimeoutSeconds() {
            return 20;
        }
    }

    @AllArgsConstructor
    private class TestFailedJobDagAction implements IJobDagAction {
        public boolean execute(final boolean success) {
            throw new IllegalStateException("This just fails");
        }

        @Override
        public int getTimeoutSeconds() {
            return 20;
        }
    }

    @AllArgsConstructor
    private class TestParallelJobAction1 implements IJobDagAction {
        @NonNull
        private final MultiThreadTestCoordinator coordinator;

        public boolean execute(final boolean success) {
            this.coordinator.nextStep();
            this.coordinator.waitUntilStep(2);
            this.coordinator.nextStep();
            return true;
        }

        @Override
        public int getTimeoutSeconds() {
            return 20;
        }

    }

    @AllArgsConstructor
    private class TestParallelJobAction2 implements IJobDagAction {
        @NonNull
        private final MultiThreadTestCoordinator coordinator;

        public boolean execute(final boolean success) {
            this.coordinator.waitUntilStep(1);
            this.coordinator.nextStep();
            this.coordinator.waitUntilStep(3);
            return true;
        }

        @Override
        public int getTimeoutSeconds() {
            return 20;
        }
    }

    @AllArgsConstructor
    private class TestParallelJobActionFailure implements IJobDagAction {
        @NonNull
        private final MultiThreadTestCoordinator coordinator;

        public boolean execute(final boolean success) {
            this.coordinator.waitUntilStep(1000);
            return true;
        }

        @Override
        public int getTimeoutSeconds() {
            return 20;
        }
    }
}
