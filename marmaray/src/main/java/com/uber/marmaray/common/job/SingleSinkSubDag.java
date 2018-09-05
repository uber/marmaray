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
package com.uber.marmaray.common.job;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.uber.marmaray.common.IPayload;
import com.uber.marmaray.common.sinks.ISink;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link SingleSinkSubDag} is used when {@link JobDag} is configured to run for single
 * {@link com.uber.marmaray.common.sinks.ISink}.
 */
@Slf4j
public class SingleSinkSubDag extends JobSubDag {

    @NonNull
    @Getter
    private final ISink sink;

    public SingleSinkSubDag(@Nonnull final ISink sink) {
        super(String.format("%s:%s", SingleSinkSubDag.class.getName(), sink.getClass().getName()));
        this.sink = sink;
    }

    @Override
    protected void executeNode(@NonNull final Optional<IPayload> data) {
        Preconditions.checkState(data.isPresent() && (data.get() instanceof DagPayload),
            "Invalid payload :" + (data.isPresent() ? data.get().getClass() : null));

        // setup job and topic metrics.
        setupMetrics();
        this.sink.write(((DagPayload) data.get()).getData());
    }

    private void setupMetrics() {
        this.sink.setJobMetrics(getJobMetrics().get());
        this.sink.setDataFeedMetrics(getDataFeedMetrics().get());
    }
}
