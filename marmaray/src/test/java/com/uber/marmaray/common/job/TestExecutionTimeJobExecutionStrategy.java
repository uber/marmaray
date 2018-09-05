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

import com.google.common.base.Optional;
import com.uber.marmaray.common.metadata.JobManagerMetadataTracker;
import org.apache.commons.lang.ArrayUtils;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestExecutionTimeJobExecutionStrategy {

    private final Map<String, Map<String, String>> defaultThings = ArrayUtils.toMap(new Object[][]{
            { "dag1", ArrayUtils.toMap(new Object[][]{
                    { JobDag.LAST_RUNTIME_METADATA_KEY, "100" },
                    { JobDag.LAST_EXECUTION_METADATA_KEY, String.valueOf(System.currentTimeMillis()) } }) },
            { "dag2", ArrayUtils.toMap(new Object[][]{
                    { JobDag.LAST_RUNTIME_METADATA_KEY, "200" },
                    { JobDag.LAST_EXECUTION_METADATA_KEY, String.valueOf(System.currentTimeMillis()) } }) },
            { "dag4", ArrayUtils.toMap(new Object[][]{
                    { "someString", "someValue" } }) },
            { "dag5", ArrayUtils.toMap(new Object[][]{
                    { JobDag.LAST_RUNTIME_METADATA_KEY, "1" },
                    { JobDag.LAST_EXECUTION_METADATA_KEY, String.valueOf(System.currentTimeMillis()
                            - TimeUnit.DAYS.toMillis(1)) } }) }
    });

    @Test
    public void testSortOrder() throws IOException {
        final JobManagerMetadataTracker tracker = mock(JobManagerMetadataTracker.class);
        when(tracker.get(any())).then(new ConfigurationAnswer());
        final IJobExecutionStrategy strategy = new ExecutionTimeJobExecutionStrategy(tracker);
        final Queue<JobDag> jobDagQueue = new ConcurrentLinkedDeque<>();
        final JobDag jobDag1 = mockJobDag("dag1");
        jobDagQueue.add(jobDag1);
        final JobDag jobDag3 = mockJobDag("dag3");
        jobDagQueue.add(jobDag3);
        final JobDag jobDag2 = mockJobDag("dag2");
        jobDagQueue.add(jobDag2);
        final JobDag jobDag4 = mockJobDag("dag4");
        jobDagQueue.add(jobDag4);
        final JobDag jobDag5 = mockJobDag("dag5");
        jobDagQueue.add(jobDag5);
        final List<JobDag> resultQueue = strategy.sort(jobDagQueue);
        final List<JobDag> expectedQueue = new ArrayList<>(jobDagQueue.size());
        // first jobs with no history and/or no success in 6 hours
        expectedQueue.add(jobDag3);
        expectedQueue.add(jobDag4);
        expectedQueue.add(jobDag5);
        // then sorted in descending order
        expectedQueue.add(jobDag2);
        expectedQueue.add(jobDag1);
        assertListEquals(expectedQueue, resultQueue);
    }

    private void assertListEquals(final List<JobDag> expectedList, final List<JobDag> resultList) {
        assertEquals(String.format("Size of queues mismatched: %s vs %s", expectedList, resultList)
                , expectedList.size(), resultList.size());
        for (int i = 0; i < expectedList.size(); i++) {
            assertEquals(String.format("Item %d of queues mismatched: %s vs %s", i, expectedList, resultList),
                    expectedList.get(i), resultList.get(i));
        }
    }

    private final class ConfigurationAnswer implements Answer {

        @Override public Optional<Map<String, String>> answer(final InvocationOnMock invocation) throws Throwable {
            final String dagName = (String) invocation.getArguments()[0];
            if (defaultThings.containsKey(dagName)) {
                return Optional.of(defaultThings.get(dagName));
            } else {
                return Optional.absent();
            }
        }
    }

    private JobDag mockJobDag(final String name) {
        final JobDag mockResult = mock(JobDag.class);
        when(mockResult.getDataFeedName()).thenReturn(name);
        when(mockResult.toString()).thenReturn(String.format("JobDag(%s)", name));
        return mockResult;
    }

}
