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

package com.uber.marmaray.common.status;

import lombok.Getter;
import lombok.NonNull;
import org.hibernate.validator.constraints.NotEmpty;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Implemenation of {@link IStatus} designed for return by {@link com.uber.marmaray.common.job.JobManager}. It maintains
 * a map of IStatus, one for each {@link com.uber.marmaray.common.job.JobDag}, or other collection of statuses to store.
 */
public class JobManagerStatus extends BaseStatus {

    @Getter
    private final Map<String, IStatus> jobStatuses = new HashMap<>();

    @Override
    public List<Exception> getExceptions() {
        final List<Exception> jobExceptions = this.jobStatuses.values().stream()
            .map(IStatus::getExceptions)
            .flatMap(List::stream)
            .collect(Collectors.toList());
        jobExceptions.addAll(super.getExceptions());
        return jobExceptions;
    }

    public void addJobStatus(@NotEmpty final String name, @NonNull final IStatus status) {
        this.jobStatuses.put(name, status);
        this.setStatus(status.getStatus());
    }
}
