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
package com.uber.marmaray.common.retry;

import com.uber.marmaray.common.configuration.SimpleRetryStrategyConfiguration;
import com.uber.marmaray.common.exceptions.RetryException;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SimpleRetryStrategy implements IRetryStrategy {
    public static final int DEFAULT_NUMBER_OF_RETRIES = 3;
    public static final long DEFAULT_WAIT_TIME_IN_MS = 1000;

    private final int numRetries;
    private final long waitTimeInMs;
    private int retriesLeft;
    private String message;

    public SimpleRetryStrategy() {
        this(DEFAULT_NUMBER_OF_RETRIES, DEFAULT_WAIT_TIME_IN_MS);
    }

    public SimpleRetryStrategy(@NonNull final SimpleRetryStrategyConfiguration conf) {
        this(conf.getNumRetries(), conf.getWaitTimeInMs());
    }

    public SimpleRetryStrategy(final int numRetries, final long waitTimeInMs) {
        this.numRetries = numRetries;
        this.waitTimeInMs = waitTimeInMs;
        this.retriesLeft = numRetries;
        this.message = "";
    }

    @Override
    public boolean shouldRetry() throws RetryException {
        this.retriesLeft--;
        this.message = "";
        if (this.retriesLeft < 0) {
            this.message = String.format("SimpleRetryStrategy failed for %d times with %d ms wait time.",
                    numRetries, waitTimeInMs);
            return false;
        }
        try {
            Thread.sleep(waitTimeInMs);
        } catch (InterruptedException e) {
            this.message = String.format("The %d-st retry failed during the %d ms wait time.",
                    numRetries - retriesLeft, waitTimeInMs);
            throw new RetryException(this.message, e);
        }
        this.message = String.format("SimpleRetryStrategy is retrying for the %d-st time, after %d ms wait time",
                numRetries - retriesLeft, waitTimeInMs);
        return true;
    }

    @Override
    public String retryMessage() {
        return this.message;
    }
}
