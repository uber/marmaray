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
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.reporters.IReporter;
import lombok.Getter;
import lombok.NonNull;
import org.hibernate.validator.constraints.NotEmpty;

import java.util.Map;

import static java.util.Objects.isNull;

/**
 * {@link Metric} encapsulates a unit of statistics either to be reported
 * by {@link IReporter} or written
 * to a UtilTable (todo)
 */
@Getter
public abstract class Metric<T> {
    @NotEmpty
    protected String metricName;
    protected T metricValue;
    protected Map<String, String> tags = Maps.newHashMap();

    protected Metric(@NotEmpty final String metricName) {
        this.metricName = metricName;
    }

    protected Metric(@NotEmpty final String metricName, @NonNull final T metricValue) {
        this(metricName);
        this.metricValue = metricValue;
    }

    public void addTag(@NotEmpty final String key, @NotEmpty final String value) {
        this.tags.put(key, value);
    }

    /**
     * Additional tag values with existing keys will replace old ones
     * @param additionalTags
     * @return
     */
    public void addTags(@NonNull final Map<String, String> additionalTags) {
        this.tags.putAll(additionalTags);
    }

    public T getMetricValue() {
        if (isNull(this.metricValue)) {
            throw new JobRuntimeException(String.format("Metric:%s", this));
        }
        return this.metricValue;
    }

    @Override
    public String toString() {
        return String.format("metricName[%s]:metricValue[%s]:tags[%s]",
                this.metricName, this.metricValue, this.tags);
    }
}
