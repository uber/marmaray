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

import com.google.common.base.Optional;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.configuration.RetryStrategyConfiguration;
import com.uber.marmaray.common.configuration.SimpleRetryStrategyConfiguration;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import lombok.NonNull;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;


class MockRetryableFunction<T,R> extends RetryableFunction<T,R> {

    private int count = 0;

    public MockRetryableFunction(IFunctionThrowsException<T,R> func,
                                 IRetryStrategy retryStrategy) {
        super(func,spy(retryStrategy));
        when(super.retryStrategy.shouldRetry()).thenCallRealMethod();
        when(super.retryStrategy.retryMessage()).thenCallRealMethod();
    }

    @Override
    protected IFunctionThrowsException<T, R> getUserFunction() {
        this.count++;
        return super.getUserFunction();
    }

    public void verifyRetry(int numFuncApply, int numShouldRetry) {
        Assert.assertEquals(count, numFuncApply);
        Mockito.verify(this.retryStrategy, times(numShouldRetry)).shouldRetry();
    }
}

public class TestRetryableFunction {
    private final static String CONFIG_YAML = "src/test/resources/config.yaml";
    private final static Configuration conf = new Configuration(new File(CONFIG_YAML), Optional.absent());
    private final static int maxRetries = new SimpleRetryStrategyConfiguration(conf).getNumRetries();

    private static int counter = 0;
    private Integer divide(@NonNull final Integer divisor) {
        counter++;
        if (divisor == 0) {
            throw new IllegalArgumentException("The divisor is 0.");
        } else {
            if (counter < maxRetries)
                throw new RuntimeException("Failing for all attempts except last one");
            return (100 / divisor);
        }
    }

    @Test
    public void testGoodRetryableFunction() throws Exception {
        counter = 0;
        final MockRetryableFunction<Integer, Integer> divideBy = new MockRetryableFunction<>(
                this::divide,
                new RetryStrategyConfiguration(conf).getRetryStrategy());
        assertEquals(new Integer(50), divideBy.apply(2));
        divideBy.verifyRetry(maxRetries, maxRetries-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBadRetryableTask() throws Exception {
        counter = 0;
        final MockRetryableFunction<Integer, Integer> divideBy = new MockRetryableFunction<>(
                this::divide,
                new RetryStrategyConfiguration(conf).getRetryStrategy());
        try {
            divideBy.apply(0);
        }
        finally {
            divideBy.verifyRetry(maxRetries, maxRetries);
        }
    }
}
