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
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.data.ForkData;
import com.uber.marmaray.common.exceptions.ForkOperationException;
import com.uber.marmaray.utilities.SparkUtil;
import com.uber.marmaray.utilities.listener.SparkJobTracker;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.RDDInfo;
import org.apache.spark.storage.StorageLevel;

import java.io.Serializable;
import java.util.List;

/**
 * {@link ForkOperator} is to be used when single RDD needs to be sent to multiple forked
 * pipelines. Each forked pipeline is associated with unique integer key and check
 * {@link ForkFunction#registerKeys(List)} more details. It calls {@link ForkFunction} for every
 * record and expects it to associate it with set of forked pipeline keys. Once entire
 * {@link ForkOperator#inputRDD}; its result is persisted using persistence level provided. If
 * nothing is specified then it will use {@link ForkOperator#DEFAULT_PERSIST_LEVEL}. Make sure to
 * call {@link ForkOperator#execute()} before calling {@link ForkOperator#getRDD(int)} or {@link #getCount(int)}.
 * Once all the data is retrieved call {@link ForkOperator#close()} to unpersist the result RDD.
 * @param <DI>
 */
@Slf4j
public class ForkOperator<DI> implements Serializable {

    public static final String PERSIST_LEVEL = Configuration.MARMARAY_PREFIX + "fork.persist_level";
    public static final String DEFAULT_PERSIST_LEVEL = "DISK_ONLY";

    private final JavaRDD<DI> inputRDD;
    private final ForkFunction<DI> forkFunction;
    private Optional<JavaRDD<ForkData<DI>>> groupRDD = Optional.absent();
    @Getter
    private final StorageLevel persistLevel;
    @Getter
    private long rddSize;
    @Getter
    private int numRddPartitions;

    public ForkOperator(@NonNull final JavaRDD<DI> inputRDD, @NonNull final ForkFunction<DI> forkFunction,
                        @NonNull final Configuration conf) {
        this.inputRDD = inputRDD;
        this.forkFunction = forkFunction;
        this.persistLevel = StorageLevel
            .fromString(conf.getProperty(PERSIST_LEVEL, DEFAULT_PERSIST_LEVEL));
    }

    public final void execute() {
        this.forkFunction.registerAccumulators(this.inputRDD.rdd().sparkContext());
        // Converts JavaRDD<T> -> JavaRDD<List<Integer>, T>
        JavaRDD<ForkData<DI>> forkedData = this.inputRDD.flatMap(this.forkFunction)
            .persist(this.persistLevel);
        final String jobName = SparkJobTracker.getJobName(this.inputRDD.rdd().sparkContext());
        forkedData.setName(String.format("%s-%s", jobName, forkedData.id()));
        // deliberately calling count so that DAG gets executed.
        final long processedRecords = forkedData.count();
        final Optional<RDDInfo> rddInfo = SparkUtil.getRddInfo(forkedData.context(), forkedData.id());
        log.info("#processed records :{} name:{}", processedRecords, forkedData.name());
        if (rddInfo.isPresent()) {
            final long size = rddInfo.get().diskSize() + rddInfo.get().memSize();
            setRddPartitionSize(size, rddInfo.get().numPartitions());
            log.info("rddInfo -> name:{} partitions:{} size:{}", forkedData.name(), rddInfo.get().numPartitions(),
                size);
        }
        this.groupRDD = Optional.of(forkedData);
    }

    // set metrics here
    private void setRddPartitionSize(final long rddSize, final int numPartitions) {
        this.rddSize = rddSize;
        this.numRddPartitions = numPartitions;
    }

    public long getCount(final int filterKey) {
        return this.forkFunction.getRecordCount(filterKey);
    }

    public JavaRDD<DI> getRDD(final int filterKey) {
        final long count = getCount(filterKey);
        log.info("#records for :{} = {}", filterKey, count);
        if (count > 0) {
            return getRDD(new FilterFunction<>(filterKey));
        } else {
            return (new JavaSparkContext(inputRDD.rdd().sparkContext())).emptyRDD();
        }
    }

    public JavaRDD<DI> getRDD(final FilterFunction<DI> filterFunction) {
        if (!this.groupRDD.isPresent()) {
            throw new ForkOperationException("No RDD is found");
        }
        return this.groupRDD.get().filter(filterFunction).map(record -> record.getRecord());
    }

    public void close() {
        if (this.groupRDD.isPresent()) {
            this.groupRDD.get().unpersist();
            this.groupRDD = Optional.absent();
        }
    }
}
