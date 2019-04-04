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

import lombok.Getter;
import lombok.ToString;
import org.hibernate.validator.constraints.NotEmpty;

/**
 * {@link Double} implementation of {@link Metric}
 */
@Getter
@ToString
public class DoubleMetric extends Metric<Double> {
    public DoubleMetric(final String metricName) {
        this("metric-type", 0.0);
    }

    public DoubleMetric(@NotEmpty final String metricName, final double metricValue) {
        super(metricName, metricValue);
        this.addTag("metric-type", "double");
    }

    public void setMetricValue(final double metricValue) {
        this.metricValue = metricValue;
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
