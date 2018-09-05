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

import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.configuration.RetryStrategyConfiguration;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link RetryableFunction#apply} will execute a given function with a given retry strategy.
 * @param <T> is the type of the input of the function;
 * @param <R> is the type of the result of the function.
 */
@Slf4j
public class RetryableFunction<T, R> {
    @NonNull protected final IRetryStrategy retryStrategy;
    @NonNull protected final IFunctionThrowsException<T, R> func;

    public RetryableFunction(@NonNull final IFunctionThrowsException<T, R> func,
                             @NonNull final IRetryStrategy retryStrategy) {
        this.func = func;
        this.retryStrategy = retryStrategy;
    }

    protected IFunctionThrowsException<T, R> getUserFunction() {
        return func;
    }

    public R apply(final T t) throws Exception {
        while (true) {
            try {
                return getUserFunction().apply(t);
            } catch (Exception e) {
                if (retryStrategy.shouldRetry()) {
                    log.info(retryStrategy.retryMessage());
                } else {
                    log.info(retryStrategy.retryMessage());
                    throw e;
                }
            }
        }
    }

    public static final class Builder<BT, BR> {
        private Configuration conf = new Configuration();
        @NonNull
        private final IFunctionThrowsException<BT, BR> func;

        public Builder(@NonNull final IFunctionThrowsException<BT, BR> func) {
            this.func = func;
        }

        public Builder withConfiguration(@NonNull final Configuration conf) {
            this.conf = conf;
            return this;
        }

        public RetryableFunction<BT, BR> build() {
            final IRetryStrategy retryStrategy = new RetryStrategyConfiguration(this.conf).getRetryStrategy();
            return new RetryableFunction<BT, BR>(this.func, retryStrategy);
        }
    }
}
