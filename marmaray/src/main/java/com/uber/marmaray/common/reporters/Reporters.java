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
package com.uber.marmaray.common.reporters;

import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.metrics.Metric;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;

@Slf4j
public final class Reporters {
    @Getter
    private final Queue<IReporter> reporters;
    private boolean isFinished = false;

    public Reporters() {
        this.reporters = new ConcurrentLinkedDeque<>();
    }

    public void addReporter(final IReporter reporter) {
        this.reporters.add(reporter);
    }

    public void report(final Metric m) {

        // Todo T1137075 - Figure out how to report metrics from spark executor side
        if (this.reporters.isEmpty()) {
            throw new JobRuntimeException("No reporters registered");
        }

        this.reporters.forEach(r -> r.gauge(m));
    }

    public void finish() {
        if (!isFinished) {
            this.reporters.forEach(r -> r.finish());
            isFinished = true;
        } else {
            log.warn("Reporters were already flushed and closed.  Please check that why this is being done again");
        }
    }
}
