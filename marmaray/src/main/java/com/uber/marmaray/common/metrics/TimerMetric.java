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

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.validator.constraints.NotEmpty;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

/**
 * {@link TimerMetric} Calculates the duration between two times in milliseconds
 *
 * {@link TimerMetric} get its value by surrounding the test subject by calling
 * {@link TimerMetric#stop()}
 */
@Slf4j
@Getter
public class TimerMetric extends LongMetric {
    private final Instant startTime;
    private Instant endTime;
    private boolean stopped;

    public TimerMetric(@NotEmpty final String metricName,
                       @NonNull final Map<String, String> tags,
                       @NonNull final Instant startTime) {
        super(metricName);
        this.addTag("metric-type", "timer");
        this.addTags(tags);
        this.startTime = startTime;
    }

    public TimerMetric(@NotEmpty final String metricName, @NonNull final Map<String, String> tags) {
        this(metricName.toString());
        this.addTags(tags);
    }

    public TimerMetric(@NotEmpty final String metricName) {
        super(metricName);
        this.addTag("metric-type", "timer");
        this.startTime = Instant.now();
    }

    public void stop() {
        if (!this.stopped) {
            this.endTime = Instant.now();
            calculateElapsed();
            this.stopped = true;
        }
    }

    @Override
    public java.lang.String toString() {
        return java.lang.String.format("%s:startTime[%s]:endTime[%s]",
                super.toString(), this.startTime, this.endTime);
    }

    private void calculateElapsed() {
        Preconditions.checkState(this.startTime.compareTo(this.endTime) <= 0);
        final Duration elapsed = Duration.between(startTime, endTime);
        log.info("{} elapsed {} milliseconds", this.metricName, elapsed.toMillis());
        super.setMetricValue(elapsed.toMillis());
    }
}
