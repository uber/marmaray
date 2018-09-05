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
package com.uber.marmaray.common.forkoperator;

import com.uber.marmaray.common.data.ForkData;
import lombok.AllArgsConstructor;
import org.apache.spark.api.java.function.Function;

/**
 * This is used for filtering result RDD. The passed in filterkey should be same as the one used
 * in ForkFunction.
 * @param <DI>
 */
@AllArgsConstructor
public class FilterFunction<DI> implements Function<ForkData<DI>, Boolean> {

    private final Integer filterKey;

    @Override
    public final Boolean call(final ForkData<DI> forkData) {
        return execute(forkData);
    }

    /**
     * It is used for filtering out tupleEntries. If it returns true then tupleEntry will be
     * filtered out. It will have same set of keys as defined by corresponding ForkFunction.
     *
     * @param forkData : forkData to be filtered out or retained.
     */
    protected Boolean execute(final ForkData<DI> forkData) {
        return forkData.getKeys().contains(this.filterKey);
    }
}
