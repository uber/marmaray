package com.uber.marmaray.utilities;

import org.apache.spark.util.AccumulatorV2;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import java.io.Serializable;

@Slf4j
public final class LongAccumulator extends AccumulatorV2<Long, Long> implements Serializable {
    @Getter
    private Long max;
    @Getter
    private Long sum;
    @Getter
    private Long count;
    @Getter
    private String name;

    public LongAccumulator(@NonNull final String name) {
        this.name = name;
        this.max = Long.MIN_VALUE;
        this.sum = 0L;
        this.count = 0L;
    }

    @Override
    public void add(@NonNull final Long v) {
        this.sum += v;
        this.count += 1;
        this.max = Math.max(max, v);
    }

    @Override
    public void reset() {
        this.sum = 0L;
        this.count = 0L;
        this.max = 0L;
    }

    public double getAvg() {
        return this.sum / this.count;
    }

    @Override
    public LongAccumulator copy() {
        final LongAccumulator longAccumulator = new LongAccumulator(this.getName());
        longAccumulator.sum = this.getSum();
        longAccumulator.count = this.getCount();
        longAccumulator.max = this.getMax();
        return longAccumulator;
    }

    @Override
    public boolean isZero() {
        return this.getSum() == 0L && this.getCount() == 0L;
    }

    @Override
    public Long value() {
        return this.getSum();
    }

    @Override
    public void merge(@NonNull final AccumulatorV2<Long, Long> other) {
        if (other instanceof LongAccumulator) {
            this.sum += ((LongAccumulator) other).getSum();
            this.count += ((LongAccumulator) other).getCount();
            this.max = Math.max(this.getMax(), ((LongAccumulator) other).getMax());
        } else {
            final String warnMsg = String.format("Cannot merge {} with {}",
                this.getClass().getName(), other.getClass().getName());
            throw new UnsupportedOperationException(warnMsg);
        }
    }

    @Override
    public String toString() {
        return String.format("name: %s, value: %s, count: %s, max:%s",
            this.getName(), this.getSum(), this.getCount(), this.getMax());
    }

    // TODO add percentile support for this accumulator to better understand distribution
}
