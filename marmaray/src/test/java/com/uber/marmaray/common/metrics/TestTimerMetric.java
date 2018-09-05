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

import com.uber.marmaray.common.exceptions.JobRuntimeException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Test for TimerMetric
 */
public class TestTimerMetric {

    private TimerMetric timerMetric;
    private final int SLEEP_TIME = 1000;
    private final int THRESHOLD = 50;

    @Before
    public void setupTestClass() {
        this.timerMetric = new TimerMetric("timer-big-function");
    }

    @Test
    public void testGetMetricValueSuccess() throws InterruptedException {
        Thread.sleep(SLEEP_TIME);
        this.timerMetric.stop();
        long diff = this.timerMetric.getMetricValue() - SLEEP_TIME;
        assertTrue(diff < THRESHOLD);
    }

    @Test(expected=JobRuntimeException.class)
    public void testGetMetricValueFail() {
        timerMetric.getMetricValue();
    }

    @Test
    public void testAddTag() {
        this.timerMetric.addTag("job", "test-job");
        assertTrue(this.timerMetric.getTags().containsKey("job"));
        assertTrue(this.timerMetric.getTags().containsKey("metric-type"));
    }
}
