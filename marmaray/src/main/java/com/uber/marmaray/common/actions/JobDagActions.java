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
package com.uber.marmaray.common.actions;

import com.uber.marmaray.common.job.ThreadPoolService;
import com.uber.marmaray.common.job.ThreadPoolServiceTier;
import com.uber.marmaray.common.metrics.LongMetric;
import com.uber.marmaray.common.reporters.Reporters;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.validator.constraints.NotEmpty;

import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.uber.marmaray.common.metrics.DataFeedMetricNames.RESULT_FAILURE;
import static com.uber.marmaray.common.metrics.DataFeedMetricNames.RESULT_SUCCESS;

/**
 * {@link JobDagActions} are actions that are run based on success status.
 * This class is completely independent and NOT an implementation of the {@link IJobDagAction} interface
 */
@Slf4j
public final class JobDagActions {
    public static final String RESULT_METRIC = "result";
    public static final String TIME_METRIC = "execution_time";

    /**
     * Class to hold and execute actions.
     *
     * Actions are executed in parallel; execution status will not affect the others.
     */

    public static final String DEFAULT_NAME = "anonymous";

    @Getter
    private final Queue<IJobDagAction> actions;
    private final Reporters reporters;

    @Getter
    private final String name;

    public JobDagActions(@NonNull final Reporters reporters) {
        this(reporters, DEFAULT_NAME);
    }

    public JobDagActions(@NonNull final Reporters reporters, @NotEmpty final String name) {
        this.actions = new ConcurrentLinkedDeque<>();
        this.name = name;
        this.reporters = reporters;
    }

    /**
     * Add an action to the container
     * @param action IAction to hold
     */
    public void addAction(@NonNull final IJobDagAction action) {
        this.actions.add(action);
    }

    /**
     * Add a collection of actions to the container
     * @param actions Collection of IActions to hold
     */
    public void addActions(@NonNull final Collection<? extends IJobDagAction> actions) {
        this.actions.addAll(actions);
    }

    /**
     * Execute all of the actions in parallel.
     *
     * Parallelism is managed by the ThreadPoolService. Throws IllegalStateException if any IAction
     * throws an Exception during execution.
     * @param dagSuccess whether the actions are responding to a successful dag run or a failed one
     */
    public boolean execute(final boolean dagSuccess) {
        final AtomicBoolean successful = new AtomicBoolean(true);
        final ConcurrentMap<Future<Boolean>, IJobDagAction> futures = new ConcurrentHashMap<>();
        this.actions.forEach(a ->
            futures.put(ThreadPoolService.submit(() -> {
                    final long startTime = System.currentTimeMillis();
                    final boolean success;
                    try {
                        success = a.execute(dagSuccess);
                    } finally {
                        final long endTime = System.currentTimeMillis();
                        reportExecuteTime(a, endTime - startTime);
                    }
                    return success;
                }, ThreadPoolServiceTier.ACTIONS_TIER, a.getTimeoutSeconds()),
                    a));
        futures.forEach((future, action) -> {
                final AtomicBoolean actionSuccess = new AtomicBoolean(true);
                try {
                    actionSuccess.set(future.get());
                } catch (Exception e) {
                    log.error("Error running JobDagAction {} for {}:", action.getClass(), this.getName(), e);
                    actionSuccess.set(false);
                    successful.set(false);
                }
                reportActionStatus(action, actionSuccess.get());
            });
        if (!successful.get()) {
            log.warn("Errors encountered during JobDagActions execution");
        }
        return successful.get();
    }

    private void reportExecuteTime(@NonNull final IJobDagAction action, final long timeInMillis) {
        final LongMetric timeMetric = new LongMetric(TIME_METRIC, TimeUnit.MILLISECONDS.toSeconds(timeInMillis));
        timeMetric.addTags(action.getMetricTags());
        this.reporters.getReporters().stream().forEach(r -> r.gauge(timeMetric));
    }

    private void reportActionStatus(@NonNull final IJobDagAction action, final boolean isSuccess) {
        final LongMetric resultMetric = new LongMetric(RESULT_METRIC, isSuccess ? RESULT_SUCCESS : RESULT_FAILURE);
        resultMetric.addTags(action.getMetricTags());
        this.reporters.getReporters().stream().forEach(r -> r.gauge(resultMetric));
    }
}
