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

package com.uber.marmaray.common.job;

import com.google.common.base.Optional;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.metadata.JobManagerMetadataTracker;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link ExecutionTimeJobExecutionStrategy} is a {@link IJobExecutionStrategy} that sorts the {@link JobDag}s by
 * execution time and latest completion, so DAGs that have not finished will run first, followed by jobs sorted by
 * execution time descending.
 * This order should help prevent individual long-running dags from keeping the execution from completing on time.
 */
@Slf4j
public class ExecutionTimeJobExecutionStrategy implements IJobExecutionStrategy {

    public static final int DEFAULT_LAST_EXECUTION_TIME_THRESHOLD_HOURS = 6;

    private final int lastExecutionTimeThresholdHours;
    private final long currentTime;

    @NonNull
    private final JobManagerMetadataTracker tracker;

    public ExecutionTimeJobExecutionStrategy(@NonNull final JobManagerMetadataTracker tracker) {
        this(tracker, DEFAULT_LAST_EXECUTION_TIME_THRESHOLD_HOURS);
    }

    public ExecutionTimeJobExecutionStrategy(@NonNull final JobManagerMetadataTracker tracker,
            final int lastExecutionTimeThresholdHours) {
        this.lastExecutionTimeThresholdHours = lastExecutionTimeThresholdHours;
        this.tracker = tracker;
        this.currentTime = System.currentTimeMillis();
    }

    @Override
    public List<Dag> sort(@NonNull final Queue<Dag> inputJobDags) {
        final Map<String, Integer> initialTopicOrdering = new HashMap<>();
        final AtomicInteger preTopicOrderingCounter = new AtomicInteger(0);
        final AtomicInteger postTopicOrderingCounter = new AtomicInteger(0);
        inputJobDags.stream().forEach(jobDag -> initialTopicOrdering.put(jobDag.getDataFeedName(),
            preTopicOrderingCounter.incrementAndGet()));
        final List<Dag> result = new ArrayList<>(inputJobDags.size());
        final long lastExecutionTimeThresholdMillis = TimeUnit.HOURS.toMillis(this.lastExecutionTimeThresholdHours);
        log.info("shuffled topic ordering");
        inputJobDags.stream().map(dag -> {
                try {
                    final Optional<Map<String, String>> contents = this.tracker.get(dag.getDataFeedName());
                    if (contents.isPresent() && contents.get().containsKey(JobDag.LAST_RUNTIME_METADATA_KEY)) {
                        long lastExecutionTime = contents.get().containsKey(JobDag.LAST_EXECUTION_METADATA_KEY)
                                ? Long.parseLong(contents.get().get(JobDag.LAST_EXECUTION_METADATA_KEY))
                                : Long.MIN_VALUE;
                        if (this.currentTime - lastExecutionTime > lastExecutionTimeThresholdMillis) {
                            return new Tuple2<>(dag, Long.MAX_VALUE);
                        }
                        return new Tuple2<>(dag, Long.valueOf(contents.get().get(JobDag.LAST_RUNTIME_METADATA_KEY)));
                    } else {
                        return new Tuple2<>(dag, Long.MAX_VALUE);
                    }
                } catch (IOException e) {
                    throw new JobRuntimeException(String.format(
                            "Unable to get metadata for dag %s : ", dag.getDataFeedName()), e);
                }
            }).sorted((o1, o2) -> o2._2().compareTo(o1._2()))
            .forEach(
                tuple -> {
                    log.info("topic ordering for {} changed from {} to {} - with weight {}",
                        tuple._1().getDataFeedName(),
                        initialTopicOrdering.get(tuple._1.getDataFeedName()),
                        postTopicOrderingCounter.incrementAndGet(),
                        tuple._2);
                    result.add(tuple._1());
                });

        return result;
    }
}
