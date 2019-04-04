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
package com.uber.marmaray.common.job;

import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

@Slf4j
public class TestThreadPoolService {

    @After
    public void shutdown() {
        // Making sure that thread pool service is stopped always after all tests.
        ThreadPoolService.shutdown(true);
    }

    // One giant test method required as the ThreadPoolService can only be initialized once
    @Test
    public void testService(){
        final Callable<Object> callable = () -> "done";

        try {
            // no thread pool service is running submit should throw exception.
            ThreadPoolService.submit(callable, ThreadPoolServiceTier.JOB_DAG_TIER);
            Assert.fail();
        } catch (IllegalStateException e) {
            // expected.
        }

        // Test for numThreads values.
        final AtomicInteger exceptionCount = new AtomicInteger(0);
        IntStream.of(0, -1).forEach(
            numThreads -> {
                try {
                    initService(numThreads, 1, 1);
                    Assert.fail();
                } catch (IllegalStateException e) {
                    exceptionCount.incrementAndGet();
                }

            }
        );
        Assert.assertEquals(2, exceptionCount.get());

        final int numThreads = 4;
        final int numJobDagThreads = 2;
        final int numActionsThreads = 2;
        initService(numThreads, numJobDagThreads, numActionsThreads);

        // re-initialization should fail (singleton pattern).
        try {
            initService(numThreads, numJobDagThreads, numActionsThreads);
            Assert.fail();
        } catch (JobRuntimeException e) {
            // expected.
        }

        // Test that caller always waits till spawned tasks have finished.
        final AtomicInteger runningCounter = new AtomicInteger(0);
        final int attempts = 3;
        final Queue<Future<Integer>> branchResults = new LinkedList<>();
        final Callable<Integer> task =
            () -> {
                int pending = attempts;
                while (pending-- > 0) {
                    runningCounter.incrementAndGet();
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        Assert.fail();
                    }
                }
                return 0;
            };
        for (int i = 0; i < numThreads; i++) {
            branchResults.add(ThreadPoolService.submit(task, ThreadPoolServiceTier.JOB_DAG_TIER));
        }
        while (!branchResults.isEmpty()) {
            try {
                final Future future = branchResults.poll();
                future.get();
                Assert.assertTrue(future.isDone());
            } catch (InterruptedException e) {
                Assert.fail();
            } catch (ExecutionException e) {
                Assert.fail();
            }
        }
        Assert.assertEquals(numThreads * attempts, runningCounter.get());

        final Callable<Integer> result = () -> 2;

        final AtomicInteger counter = new AtomicInteger(0);
        final Queue<Future<Integer>> results = new LinkedList<>();
        final Callable<Integer> failingTask =
            () -> {
                counter.incrementAndGet();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    log.error("I was interrupted???");
                }
                return 3/0;
            };
        final int jobDagCount = 100 * numJobDagThreads;
        final int actionsCount = 100 * numActionsThreads;
        for (int i = 0; i < jobDagCount; i++) {
            results.add(ThreadPoolService.submit(failingTask, ThreadPoolServiceTier.JOB_DAG_TIER));
        }
        for (int i = 0; i < actionsCount; i++) {
            results.add(ThreadPoolService.submit(failingTask, ThreadPoolServiceTier.ACTIONS_TIER));
        }
        // wait for threads to stop.
        ThreadPoolService.shutdown(false);
        // make sure we scheduled them all
        Assert.assertEquals(jobDagCount + actionsCount, results.size());
        // make sure they all ran
        Assert.assertEquals(jobDagCount + actionsCount, counter.get());
        exceptionCount.set(0);
        while (!results.isEmpty()) {
            final Future<Integer> status = results.poll();
            try {
                status.get();
                Assert.fail();
            } catch (InterruptedException e) {
                Assert.fail();
            } catch (ExecutionException e) {
                exceptionCount.incrementAndGet();
                // expected.
                Assert.assertEquals(ArithmeticException.class, e.getCause().getClass());
            }
        }
        // make sure they all failed
        Assert.assertEquals(jobDagCount + actionsCount, exceptionCount.get());
    }

    @Test
    public void testTimeouts() throws Exception {
        ThreadPoolService.init(new Configuration());
        final AtomicInteger counter = new AtomicInteger();
        final Callable sleepCallable = () -> {
            Thread.sleep(10000);
            counter.incrementAndGet();
            return 1;
        };
        Future future = ThreadPoolService.submit(sleepCallable, ThreadPoolServiceTier.ACTIONS_TIER, 1);
        try {
            future.get();
            Assert.fail("Exception should have been thrown");
        } catch (JobRuntimeException e) {
            // expected
            assert(e.getCause() instanceof TimeoutException);
        }
        Assert.assertEquals(0, counter.get());
        future = ThreadPoolService.submit(sleepCallable, ThreadPoolServiceTier.ACTIONS_TIER, 1000);
        try {
            future.get(1, TimeUnit.SECONDS);
            Assert.fail("Exception should have been thrown");
        } catch (TimeoutException e) {
            // expected
        }
        Assert.assertEquals(0, counter.get());
    }

    @Test
    public void testCancel() throws InterruptedException {
        final Configuration conf = new Configuration();
        initService(2, 1, 1);
        final AtomicInteger counter = new AtomicInteger();
        final Callable sleepCallable = () -> {
            Thread.sleep(10000);
            counter.incrementAndGet();
            return 1;
        };
        // thread should actually run
        final Future future1 = ThreadPoolService.submit(sleepCallable, ThreadPoolServiceTier.ACTIONS_TIER);
        // thread should be queued and not run immediately
        final Future future2 = ThreadPoolService.submit(sleepCallable, ThreadPoolServiceTier.ACTIONS_TIER);
        Thread.sleep(1000);
        Assert.assertTrue(future1.cancel(true));
        Assert.assertTrue(future2.cancel(true));
        ThreadPoolService.shutdown(false);
        Assert.assertTrue(future1.isCancelled());
        Assert.assertTrue(future2.isCancelled());
        Assert.assertEquals(0, counter.get());
        Assert.assertTrue(future1.isDone());
        Assert.assertTrue(future2.isDone());
    }

    @Test
    public void testActionsCanExpand() throws Exception{
        final int numThreads = 4;
        final int numActionsThreads = 1;
        final int numJobDagThreads = 1;
        final int sleepMillis = 2000;
        initService(numThreads, numJobDagThreads, numActionsThreads);
        final AtomicInteger startedActions = new AtomicInteger();
        final AtomicInteger startedJobDags = new AtomicInteger();
        final Callable actionsCallable = () -> {
            startedActions.incrementAndGet();
            Thread.sleep(sleepMillis);
            return 1;
        };
        final Callable jobDagCallable = () -> {
            startedJobDags.incrementAndGet();
            Thread.sleep(sleepMillis);
            return 1;
        };
        // start two threads, taking up one space in the shared queue
        for (int i = 0; i < 2; i++) {
            ThreadPoolService.submit(jobDagCallable, ThreadPoolServiceTier.JOB_DAG_TIER);
        }
        for (int i = 0; i < 10; i++) {
            ThreadPoolService.submit(actionsCallable, ThreadPoolServiceTier.ACTIONS_TIER);
        }
        // make sure things have started
        Thread.sleep(1000);
        // assert that we have 3 actions and 1 job dag running
        Assert.assertEquals(2, startedActions.get());
        Assert.assertEquals(2, startedJobDags.get());
        // wait for one round to finish; we should have queued 3 more actions
        Thread.sleep(sleepMillis);
        Assert.assertEquals(5, startedActions.get());
    }

    private void initService(final int numThreads, final int numJobDagThreads, final int numActionsThreads) {
        final Configuration conf = new Configuration();
        conf.setProperty(ThreadPoolService.NUM_THREADS, String.valueOf(numThreads));
        conf.setProperty(ThreadPoolService.JOB_DAG_THREADS, String.valueOf(numJobDagThreads));
        conf.setProperty(ThreadPoolService.ACTIONS_THREADS, String.valueOf(numActionsThreads));
        ThreadPoolService.init(conf);
    }

}
