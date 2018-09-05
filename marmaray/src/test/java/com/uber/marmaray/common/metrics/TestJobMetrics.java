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
package com.uber.marmaray.common.metrics;

import com.google.common.collect.Maps;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link JobMetrics}
 */
public class TestJobMetrics {
    private static JobMetrics jobMetrics = new JobMetrics("test-job");

    @Test
    public void testCreateJobLongMetric() {
        Metric jobMetric1 = jobMetrics.createJobLongMetric(JobMetricType.DRIVER_MEMORY, 100l, Maps.newHashMap());
        Metric jobMetric2 = jobMetrics.createJobLongMetric(JobMetricType.SIZE, 1028l, Maps.newHashMap());

        assertEquals("job-driver_memory", jobMetric1.getMetricName());
        assertEquals("job-size", jobMetric2.getMetricName());

        assertEquals(new Long(100), jobMetric1.getMetricValue());
        assertEquals(new Long(1028), jobMetric2.getMetricValue());

        assertTrue(jobMetric1.getTags().containsKey("job"));
        assertEquals(jobMetric1.getTags().get("job"), "test-job");
        assertFalse(jobMetric1.getTags().containsKey("topic"));
        assertFalse(jobMetric1.getTags().containsKey("hello"));
        jobMetric1.addTag("hello", "world");
        assertTrue(jobMetric1.getTags().containsKey("hello"));

        assertTrue(jobMetrics.getMetricSet().contains(jobMetric1));
        assertTrue(jobMetrics.getMetricSet().contains(jobMetric2));
    }

    @Test
    public void testCreateJobTimerMetric() {
        Metric jobMetric1 = jobMetrics.createJobTimerMetric(JobMetricType.RUNTIME, Maps.newHashMap());
        Metric jobMetric2 = jobMetrics.createJobTimerMetric(JobMetricType.STAGE_RUNTIME, Maps.newHashMap());

        assertEquals("job-runtime", jobMetric1.getMetricName());
        assertEquals("job-stage_runtime", jobMetric2.getMetricName());

        assertTrue(jobMetric1 instanceof TimerMetric);
        assertTrue(jobMetric2 instanceof TimerMetric);

        assertTrue(jobMetric1.getTags().containsKey("job"));
        assertEquals(jobMetric1.getTags().get("job"), "test-job");
        assertFalse(jobMetric1.getTags().containsKey("topic"));
        assertFalse(jobMetric1.getTags().containsKey("hello"));
        jobMetric1.addTag("hello", "world");
        assertTrue(jobMetric1.getTags().containsKey("hello"));

        assertTrue(jobMetrics.getMetricSet().contains(jobMetric1));
        assertTrue(jobMetrics.getMetricSet().contains(jobMetric2));
    }

}
