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
package com.uber.marmaray.common.actions;

import com.uber.marmaray.common.metrics.DataFeedMetrics;
import com.uber.marmaray.common.metrics.JobMetrics;
import com.uber.marmaray.common.reporters.Reporters;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link ReporterAction} is an implemetation of the {@link IJobDagAction} interface and is used to
 * report metrics
 */
@Slf4j
@AllArgsConstructor
public class ReporterAction implements IJobDagAction {

    public static int DEFAULT_TIMEOUT_SECONDS = 120;

    @Getter
    private final Reporters reporters;
    @Getter
    private final JobMetrics jobMetrics;
    private final DataFeedMetrics dataFeedMetrics;

    @Override
    public boolean execute(final boolean success) {
        if (success) {
            this.reporters.getReporters().forEach(reporter -> {
                    this.jobMetrics.gaugeAll(reporter);
                    this.dataFeedMetrics.gaugeAll(reporter);
                });
        } else {
            this.reporters.getReporters().forEach(reporter -> this.dataFeedMetrics.gauageFailureMetric(reporter));
            log.warn("Other than failure reports "
                    + "no metrics produced or actions being executed on reporter because errors were encountered");
        }
        return success;
    }

    @Override
    public int getTimeoutSeconds() {
        return DEFAULT_TIMEOUT_SECONDS;
    }
}
