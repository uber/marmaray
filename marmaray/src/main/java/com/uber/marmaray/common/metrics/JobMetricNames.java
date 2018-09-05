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

import com.uber.marmaray.common.exceptions.JobRuntimeException;

public final class JobMetricNames {
    public static final String RUN_JOB_DAG_LATENCY_MS = "run_job_dag_latency_ms";
    public static final String RUN_JOB_ERROR_COUNT = "run_job_error_count";
    public static final String JOB_SETUP_LATENCY_MS = "job_setup_latency_ms";

    // JobLockManager-related metrics
    public static final String JOB_MANAGER_LOCK_TIME_MS = "job_manager_lock_time_ms";
    public static final String JOB_DAG_LOCK_TIME_MS = "job_dag_lock_time_ms";

    private JobMetricNames() {
        throw new JobRuntimeException("Class should never be instantiated");
    }
}
