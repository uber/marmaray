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
package com.uber.marmaray.common.dataset;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.hibernate.validator.constraints.NotEmpty;

/**
 * Simple Java Bean used to construct {@link UtilTable} of {@ExceptionRecord}
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class ExceptionRecord extends UtilRecord {
    private String exception;
    private String exception_message;
    private String stacktrace;
    private boolean detected_on_driver;

    public ExceptionRecord(@NotEmpty final String applicationId,
                           @NotEmpty final String jobName,
                           final long jobStartTimestamp,
                           final long timestamp,
                           @NotEmpty final String exception,
                           @NotEmpty final String exceptionMessage,
                           @NotEmpty final String stacktrace,
                           final boolean isDriver) {
        super(applicationId, jobName, jobStartTimestamp, timestamp);
        this.exception = exception;
        this.exception_message = exceptionMessage;
        this.stacktrace = stacktrace;
        this.detected_on_driver = isDriver;
    }
}
