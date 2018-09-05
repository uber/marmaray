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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.uber.marmaray.common.reporters.Reportable;
import com.uber.marmaray.common.reporters.IReporter;
import lombok.Getter;
import lombok.NonNull;
import org.hibernate.validator.constraints.NotEmpty;

import java.util.Map;
import java.util.Set;

/**
 * Job level metric should be created via {@link JobMetrics} using {@link JobMetrics#createJobLongMetric}
 * and {@link JobMetrics#createJobTimerMetric}, and reported using {@link JobMetrics#gaugeAll}
 */
@Getter
public class JobMetrics implements Reportable {
    private static final String METRIC_PREFIX = "job-";
    private static final String JOB_TAG = "job";
    private final Set<Metric> metricSet = Sets.newHashSet();
    private final Map<String, String> baseTags;
    @NotEmpty
    private final String jobName;

    public JobMetrics(final String jobName) {
        this.jobName = jobName;
        this.baseTags = ImmutableMap.of(JOB_TAG, jobName);
    }

    public LongMetric createJobLongMetric(@NonNull final JobMetricType metricType,
                                          final long metricValue,
                                          @NonNull final Map<String, String> additionalTags) {
        final LongMetric metric = new LongMetric(getMetricName(metricType), metricValue);
        metric.addTags(getBaseTags());
        metric.addTags(additionalTags);
        this.metricSet.add(metric);
        return metric;
    }

    public TimerMetric createJobTimerMetric(@NonNull final JobMetricType metricType,
                                            @NonNull final Map<String, String> additionalTags) {
        final TimerMetric metric = new TimerMetric(getMetricName(metricType));
        metric.addTags(getBaseTags());
        metric.addTags(additionalTags);
        metricSet.add(metric);
        return metric;
    }

    private static String getMetricName(@NonNull final JobMetricType metricType) {
        return METRIC_PREFIX + metricType.toString().toLowerCase();
    }

    @Override
    public void gaugeAll(@NonNull final IReporter reporter) {
        metricSet.forEach(reporter::gauge);
    }
}
