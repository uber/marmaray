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

/**
 * Known {@link DataFeedMetrics} names.
 */
public final class DataFeedMetricNames {
    // metrics in context of the job
    public static final String RUNTIME = "runtime";
    public static final String SIZE_BYTES = "size_bytes";
    public static final String CURRENT_STATUS = "current_status";

    public static final String AVAILABLE_ROWCOUNT = "available_rowcount";
    public static final String INPUT_ROWCOUNT = "input_rowcount";
    public static final String OUTPUT_ROWCOUNT = "output_rowcount";
    public static final String OUTPUT_BYTE_SIZE = "output_byte_size";
    public static final String ERROR_ROWCOUNT = "error_rowcount";
    public static final String DUPLICATE_ROWCOUNT = "duplicate_rowcount";
    public static final String BULK_INSERT_COUNT = "insert_count";
    public static final String UPSERT_COUNT = "upsert_count";

    public static final String CHANGED_ROWCOUNT = "changed_rowcount";
    public static final String NON_CONFORMING_ROWCOUNT = "non_conforming_rowcount";

    // Used to indicate if job succeeded or not.
    public static final String RESULT = "result";
    public static final int RESULT_SUCCESS = 1;
    public static final int RESULT_FAILURE = -1;

    public static final String DISPERSAL_CONFIGURATION_INIT_ERRORS = "dispersal_config_error_count";

    // metrics in context of data flow
    public static final String FRESHNESS = "freshness";
    public static final String INTERVAL_INPUT_ROWCOUNT = "interval_input_rowcount";
    public static final String INTERVAL_OUTPUT_ROWCOUNT = "interval_output_rowcount";
    public static final String ROWCOUNT_BEHIND = "rowcount_behind";

    // Timer related metrics
    public static final String INIT_CONFIG_LATENCY_MS = "init_config_latency_ms";
    public static final String INIT_METADATAMANAGER_LATENCY_MS = "init_metadatamanager_latency_ms";
    public static final String CONVERT_SCHEMA_LATENCY_MS = "convert_schema_latency_ms";
    public static final String TOTAL_LATENCY_MS = "total_latency_ms";

    private DataFeedMetricNames() {
        throw new JobRuntimeException("This class should never be instantiated");
    }
}

