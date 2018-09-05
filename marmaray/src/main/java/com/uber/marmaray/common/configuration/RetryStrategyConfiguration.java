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

import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.retry.IRetryStrategy;
import com.uber.marmaray.common.retry.SimpleRetryStrategy;

import java.io.Serializable;

import lombok.NonNull;

/**
 * {@link RetryStrategyConfiguration} defines configurations related to the retry strategy.
 *
 * All properties start with {@link #RETRY_STRATEGY_PREFIX}.
 */
public class RetryStrategyConfiguration implements Serializable {
    public static final String RETRY_STRATEGY_PREFIX = Configuration.MARMARAY_PREFIX + "retry_strategy.";
    public static final String DEFAULT_STRATEGY = RETRY_STRATEGY_PREFIX + "default_strategy";

    public static final String SIMPLE_RETRY_STRATEGY = "SimpleRetryStrategy";

    private final Configuration conf;

    public RetryStrategyConfiguration(@NonNull final Configuration conf) {
        this.conf = conf;
    }

    public IRetryStrategy getRetryStrategy() {
        final String strategy = conf.getProperty(DEFAULT_STRATEGY, SIMPLE_RETRY_STRATEGY);

        switch (strategy) {
            case SIMPLE_RETRY_STRATEGY:
                return new SimpleRetryStrategy(new SimpleRetryStrategyConfiguration(conf));
            default:
                throw new JobRuntimeException(String.format("Retry strategy %s is not supported.", strategy));
        }
    }
}
