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

import com.google.common.base.Optional;
import com.uber.marmaray.common.data.ForkData;
import com.uber.marmaray.common.exceptions.ForkOperationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.util.LongAccumulator;

/**
 * It associates every record with a set of fork-keys. This is an abstract class and subclass of
 * it should implement {@link ForkFunction#process(Object)} method. All the keys used by
 * {@link ForkFunction} should be registered by calling {@link ForkFunction#registerKeys(List)}. If
 * any non registered key is returned by {@link ForkFunction#process(Object)} then it will
 * raise {@link ForkOperationException}.
 * @param <DI>
 */
@Slf4j
public abstract class ForkFunction<DI> implements FlatMapFunction<DI, ForkData<DI>> {

    private final Map<Integer, Optional<LongAccumulator>> registeredKeys = new HashMap<>();

    public void registerKeys(final List<Integer> keys) {
        keys.stream().forEach(key -> {
                if (this.registeredKeys.containsKey(key)) {
                    throw new ForkOperationException("Duplicate key found :" + key);
                }
                this.registeredKeys.put(key, Optional.absent());
            });
    }

    public long getRecordCount(final int key) {
        final Optional<LongAccumulator> keyCount = this.registeredKeys.get(key);
        final long ret = keyCount.isPresent() ? keyCount.get().count() : 0;
        log.info("{} : key :{}: count :{}", this.getClass().getName(), key, ret);
        return ret;
    }

    public void registerAccumulators(@NonNull final SparkContext sparkContext) {
        this.registeredKeys.entrySet().forEach(
            entry -> {
                entry.setValue(Optional.of(sparkContext.longAccumulator()));
            }
        );
    }

    @Override
    public final Iterator<ForkData<DI>> call(final DI di) {
        final List<ForkData<DI>> forkData = process(di);
        forkData.stream().forEach(fd -> verifyKeys(fd.getKeys(), di));
        return forkData.iterator();
    }

    private void verifyKeys(final List<Integer> keys, final DI di) {
        keys.stream().forEach(key -> {
                if (!this.registeredKeys.containsKey(key)) {
                    log.error("Invalid key:{}: in keys:{}:for record:{}", key, keys, di);
                    throw new ForkOperationException("Using unregistered key :" + key);
                }
                this.registeredKeys.get(key).get().add(1);
            });
    }

    /**
     * This method should be implemented by subclass. This method is suppose to associate every
     * record with set of registered keys.
     * @return  {@link ForkData} with set of fork keys which should be associated with the record.
     * @param record element from {@link ForkOperator#inputRDD}.
     * @throws Exception
     */
    protected abstract List<ForkData<DI>> process(final DI record);
}
