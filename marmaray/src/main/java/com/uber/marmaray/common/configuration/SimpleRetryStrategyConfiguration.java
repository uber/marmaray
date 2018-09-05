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
package com.uber.marmaray.common.configuration;

import com.uber.marmaray.common.retry.SimpleRetryStrategy;

import java.io.Serializable;

import lombok.NonNull;

/**
 * {@link SimpleRetryStrategyConfiguration} defines configurations related to a retry strategy based on
 * total number of retries and time to wait in between retries.
 *
 * All properties start with {@link #SIMPLE_RETRY_PREFIX}.
 */
public class SimpleRetryStrategyConfiguration implements Serializable {
    private static final String SIMPLE_RETRY_PREFIX = RetryStrategyConfiguration.RETRY_STRATEGY_PREFIX + "simple.";
    private static final String NUM_OF_RETRIES = SIMPLE_RETRY_PREFIX + "num_of_retries";
    private static final String WAIT_TIME_IN_MS = SIMPLE_RETRY_PREFIX + "wait_time_in_ms";

    private final Configuration conf;

    public SimpleRetryStrategyConfiguration(@NonNull final Configuration conf) {
        this.conf = conf;
    }

    public int getNumRetries() {
        return conf.getIntProperty(NUM_OF_RETRIES, SimpleRetryStrategy.DEFAULT_NUMBER_OF_RETRIES);
    }

    public long getWaitTimeInMs() {
        return conf.getLongProperty(WAIT_TIME_IN_MS, SimpleRetryStrategy.DEFAULT_WAIT_TIME_IN_MS);
    }
}
