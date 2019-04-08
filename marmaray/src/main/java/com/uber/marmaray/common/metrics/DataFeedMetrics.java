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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.uber.marmaray.common.reporters.IReporter;
import com.uber.marmaray.common.reporters.Reportable;
import lombok.Getter;
import lombok.NonNull;
import org.hibernate.validator.constraints.NotEmpty;

import java.io.Serializable;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Topic level metric should be created via {@link DataFeedMetrics} using {@link DataFeedMetrics#createLongMetric}
 * and {@link DataFeedMetrics#createTimerMetric}, and reported using {@link DataFeedMetrics#gaugeAll}
 */
@Getter
public class DataFeedMetrics implements Reportable, Serializable {

    public static final String DATA_FEED_NAME = "datafeed";

    private static final String JOB_TAG = "job";

    @Getter
    private final Set<Metric> metricSet = Sets.newHashSet();

    @Getter
    private final Set<Metric> failureMetricSet = Sets.newHashSet();

    private final Map<String, String> baseTags;

    @NotEmpty
    private final String jobName;

    public DataFeedMetrics(final String jobName, final Map<String, String> tags) {
        this.jobName = jobName;
        final Map<String, String> tagsMap = Maps.newHashMap(tags);
        tagsMap.put(JOB_TAG, jobName);
        this.baseTags = Collections.unmodifiableMap(tagsMap);
    }

    public LongMetric createLongMetric(@NonNull final String metricName,
                                       final long metricValue,
                                       @NonNull final Map<String, String> additionalTags) {
        final LongMetric metric = new LongMetric(metricName, metricValue);
        metric.addTags(getBaseTags());
        metric.addTags(additionalTags);
        this.metricSet.add(metric);
        return metric;
    }

    public LongMetric createLongMetric(@NonNull final String metricName,
                                       final long metricValue) {
        return createLongMetric(metricName, metricValue, new HashMap<>());
    }

    public LongMetric createLongFailureMetric(@NonNull final String metricName,
                                              final long metricValue,
                                              @NonNull final Map<String, String> additionalTags) {
        final LongMetric metric = new LongMetric(metricName, metricValue);
        metric.addTags(getBaseTags());
        metric.addTags(additionalTags);
        this.failureMetricSet.add(metric);
        return metric;
    }

    public LongMetric createLongFailureMetric(@NonNull final String metricName,
                                              final long metricValue) {
        return createLongFailureMetric(metricName, metricValue, new HashMap<>());
    }

    public TimerMetric createTimerMetric(@NonNull final String metricName) {
        final TimerMetric metric = new TimerMetric(metricName);
        metric.addTags(getBaseTags());
        metricSet.add(metric);
        return metric;
    }

    public TimerMetric createTimerMetric(@NonNull final String metricName,
                                         @NonNull final Map<String, String> additionalTags,
                                         @NonNull final Optional<Instant> startTime) {
        final TimerMetric metric = startTime.isPresent()
                ? new TimerMetric(metricName, additionalTags, startTime.get())
                : new TimerMetric(metricName);
        metric.addTags(getBaseTags());
        metric.addTags(additionalTags);
        metricSet.add(metric);
        return metric;
    }

    public static Map<String, String> createAdditionalTags(@NonNull final String key, @NonNull final String val) {
        final Map<String, String> additionalTag = new HashMap<>();
        additionalTag.put(key, val);
        return additionalTag;
    }

    public void gauageFailureMetric(final IReporter reporter) {
        this.failureMetricSet.forEach(reporter::gauge);
    }

    public void guageNonFailureMetric(final IReporter reporter) {
        this.metricSet.forEach(reporter::gauge);
    }

    public void gaugeAll(final IReporter reporter) {
        guageNonFailureMetric(reporter);
        gauageFailureMetric(reporter);
    }
}
