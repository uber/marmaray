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
package com.uber.marmaray.utilities.listener;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.Stage;
import scala.collection.Iterator;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TimeoutManager
 *
 * This class tracks the total running time of the Spark job and the last activity in every individual stages.
 * If the job times out, TimeoutManager will kill all spark stages.
 * If an individual stage is inactive for a long time, TimeoutManager kills the stage and all related jobs.
 *
 */
@Slf4j
public final class TimeoutManager {

    public static final String TIMEOUT_MANAGER_PREFIX = Configuration.MARMARAY_PREFIX + "timeout_manager.";
    public static final String JOB_TIMEOUT_IN_MINS = TIMEOUT_MANAGER_PREFIX + "job_timeout_in_mins";
    public static final String STAGE_STALLED_IN_MINS = TIMEOUT_MANAGER_PREFIX + "stage_stalled_in_mins";
    public static final int DEFAULT_JOB_TIMEOUT_IN_MINS = (int) TimeUnit.HOURS.toMinutes(3);
    public static final int DEFAULT_STAGE_STALLED_IN_MINS = (int) TimeUnit.HOURS.toMinutes(3);

    // checking whether the job has timed out every 1 minute
    public static final long FREQUENCY_IN_MINS = 1;
    public static final TimeUnit FREQUENCY_UNIT = TimeUnit.MINUTES;

    private static TimeoutManager instance = null;

    @Getter
    private static Boolean timedOut;

    private final long jobTimeoutMillis;
    private final long stageTimeoutMillis;
    private final SparkContext sc;

    private final long startTime;
    @VisibleForTesting
    @Getter
    private final Map<Integer, StageActivityTracker> lastActiveTime = new ConcurrentHashMap<>();

    private TimeoutManager(final int jobTimeoutInMins, final int stageStalledInMins, @NonNull final SparkContext sc) {
        this.jobTimeoutMillis = TimeUnit.MINUTES.toMillis(jobTimeoutInMins);
        this.stageTimeoutMillis = TimeUnit.MINUTES.toMillis(stageStalledInMins);
        this.sc = sc;
        this.startTime = getCurrentTime();
        this.timedOut = false;
        log.info("Initializing TimeoutManager, job_timeout = {}ms, stage_timeout = {}ms",
                this.jobTimeoutMillis, this.stageTimeoutMillis);
    }

    public static TimeoutManager getInstance() {
        if (instance == null) {
            throw new JobRuntimeException("TimeoutManager has not been initialized...");
        } else {
            return instance;
        }
    }

    public static synchronized void init(@NonNull final Configuration conf, @NonNull final SparkContext sc) {
        if (instance != null) {
            log.info("TimeoutManager instance already exists, job_timeout = {}ms, stage_timeout = {}ms",
                    instance.jobTimeoutMillis, instance.stageTimeoutMillis);
        } else {
            final int jobTimeoutInMins = conf.getIntProperty(JOB_TIMEOUT_IN_MINS, DEFAULT_JOB_TIMEOUT_IN_MINS);
            final int stageStalledInMins = conf.getIntProperty(STAGE_STALLED_IN_MINS, DEFAULT_STAGE_STALLED_IN_MINS);
            instance = new TimeoutManager(jobTimeoutInMins, stageStalledInMins, sc);
        }
    }

    public static synchronized void close() {
        instance = null;
    }

    public void startMonitorThread() {
        log.info("Start timeout monitoring...");
        final Thread monitor = new Thread(() -> monitorTimeout());
        monitor.setDaemon(true);
        monitor.start();
    }

    private void monitorTimeout() {
        try {
            while (true) {
                FREQUENCY_UNIT.sleep(FREQUENCY_IN_MINS);
                log.info("Checking whether the job or any Spark stage has timed out...");

                if (jobTimeout()) {
                    log.error("The spark job is taking longer than {} ms. Cancelling all jobs...",
                            this.jobTimeoutMillis);
                    this.timedOut = true;
                    this.sc.cancelAllJobs();
                    throw new TimeoutException("The spark job is timing out");
                }

                final List<Stage> stalledStages = this.stalledStages();
                if (stalledStages.size() > 0) {
                    for (Stage stage : stalledStages) {
                        log.error("Cancelling stage {}-{} and its related jobs due to inactivity... details: {}",
                                stage.id(), stage.name(), stage.details());
                        this.timedOut = true;
                        this.sc.cancelStage(stage.id());
                    }
                }
                log.info("The job and all stages are running fine within the timeout limits.");
            }
        } catch (InterruptedException | TimeoutException e) {
            log.info("Shutting down timeout monitor thread");
            throw new JobRuntimeException(e);
        }
    }

    public void stageFinished(final int stageId) {
        this.lastActiveTime.remove(stageId);
    }

    public void stageStarted(final int stageId) {
        this.lastActiveTime.put(stageId, new StageActivityTracker());
    }

    public void taskStarted(final int stageId) {
        Optional<StageActivityTracker> stageTracker = Optional.fromNullable(lastActiveTime.get(stageId));
        if (!stageTracker.isPresent()) {
            stageStarted(stageId);
            stageTracker = Optional.fromNullable(lastActiveTime.get(stageId));
        }
        stageTracker.get().taskStarted();
    }

    public void taskFinished(final int stageId) {
        final Optional<StageActivityTracker> stageTracker = Optional.fromNullable(lastActiveTime.get(stageId));
        if (stageTracker.isPresent()) {
            stageTracker.get().taskFinished();
        }
    }

    public boolean jobTimeout() {
        return (getCurrentTime() - startTime > jobTimeoutMillis);
    }

    public List<Stage> stalledStages() {
        final List<Stage> stalledStages = new LinkedList<>();
        final long currentTime = getCurrentTime();
        final Iterator<Stage> stageItr = sc.dagScheduler().runningStages().iterator();
        while (stageItr.hasNext()) {
            final Stage stage = stageItr.next();
            final int stageId = stage.id();
            final Optional<StageActivityTracker> stageTracker = Optional.fromNullable(lastActiveTime.get(stageId));
            if (stageTracker.isPresent() && stageTracker.get().getRunningTasks().get() > 0) {
                if (currentTime - lastActiveTime.get(stageId).lastActiveTime > stageTimeoutMillis) {
                    stalledStages.add(stage);
                }
            }
        }
        return stalledStages;
    }

    private static long getCurrentTime() {
        return System.currentTimeMillis();
    }

    /**
     * Helper class for tracking Spark stage's activity.
     */
    @Getter
    @RequiredArgsConstructor
    public static class StageActivityTracker {
        @VisibleForTesting
        @Getter
        private final AtomicInteger runningTasks = new AtomicInteger(0);
        private long lastActiveTime = 0L;

        public void taskStarted() {
            this.lastActiveTime = getCurrentTime();
            this.runningTasks.incrementAndGet();
        }

        public void taskFinished() {
            this.lastActiveTime = getCurrentTime();
            this.runningTasks.decrementAndGet();
        }

    }
}
