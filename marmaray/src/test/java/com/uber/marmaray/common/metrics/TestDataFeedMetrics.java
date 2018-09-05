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

import com.google.common.base.Optional;
import org.junit.Test;
import org.spark_project.guava.collect.Maps;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link DataFeedMetrics}
 */
public class TestDataFeedMetrics {
    private static DataFeedMetrics dataFeedMetrics = new DataFeedMetrics("test-job",
            Collections.singletonMap(DataFeedMetrics.DATA_FEED_NAME, "test-topic"));

    @Test
    public void testCreateTopicLongMetric() {
        Metric topicMetric1 = dataFeedMetrics.createLongMetric(DataFeedMetricNames.OUTPUT_ROWCOUNT,
                100l,
                Maps.newHashMap());
        Metric topicMetric2 = dataFeedMetrics.createLongMetric(DataFeedMetricNames.SIZE_BYTES,
                1028l,
                Maps.newHashMap());

        assertEquals("output_rowcount", topicMetric1.getMetricName());
        assertEquals("size_bytes", topicMetric2.getMetricName());

        assertEquals(new Long(100), topicMetric1.getMetricValue());
        assertEquals(new Long(1028), topicMetric2.getMetricValue());

        assertTrue(topicMetric1.getTags().containsKey("job"));
        assertEquals(topicMetric1.getTags().get("job"), "test-job");
        assertTrue(topicMetric1.getTags().containsKey("datafeed"));
        assertEquals(topicMetric1.getTags().get("datafeed"), "test-topic");
        assertFalse(topicMetric1.getTags().containsKey("hello"));
        topicMetric1.addTag("hello", "world");
        assertTrue(topicMetric1.getTags().containsKey("hello"));

        assertTrue(dataFeedMetrics.getMetricSet().contains(topicMetric1));
        assertTrue(dataFeedMetrics.getMetricSet().contains(topicMetric2));
    }

    @Test
    public void testCreateTopicTimerMetric() {
        Metric topicMetric1 = dataFeedMetrics.createTimerMetric(DataFeedMetricNames.RUNTIME);

        assertEquals("runtime", topicMetric1.getMetricName());

        assertTrue(topicMetric1 instanceof TimerMetric);

        assertTrue(topicMetric1.getTags().containsKey("job"));
        assertEquals(topicMetric1.getTags().get("job"), "test-job");
        assertTrue(topicMetric1.getTags().containsKey("datafeed"));
        assertEquals(topicMetric1.getTags().get("datafeed"), "test-topic");
        assertFalse(topicMetric1.getTags().containsKey("hello"));
        topicMetric1.addTag("hello", "world");
        assertTrue(topicMetric1.getTags().containsKey("hello"));

        assertTrue(dataFeedMetrics.getMetricSet().contains(topicMetric1));
    }

}
