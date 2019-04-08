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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.uber.marmaray.common.actions.IJobDagAction;
import com.uber.marmaray.common.actions.JobDagActions;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.exceptions.MetadataException;
import com.uber.marmaray.common.metadata.JobManagerMetadataTracker;
import com.uber.marmaray.common.metrics.JobMetricNames;
import com.uber.marmaray.common.metrics.JobMetrics;
import com.uber.marmaray.common.metrics.TimerMetric;
import com.uber.marmaray.common.reporters.ConsoleReporter;
import com.uber.marmaray.common.reporters.IReporter;
import com.uber.marmaray.common.reporters.Reporters;
import com.uber.marmaray.common.spark.SparkFactory;
import com.uber.marmaray.common.status.IStatus;
import com.uber.marmaray.common.status.JobManagerStatus;
import com.uber.marmaray.utilities.LockManager;
import com.uber.marmaray.utilities.listener.SparkJobTracker;
import com.uber.marmaray.utilities.listener.TimeoutManager;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.avro.mapred.Pair;
import org.hibernate.validator.constraints.NotEmpty;

import java.util.Collection;
import java.util.HashMap;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * JobManager
 *
 * This class is responsible for running the entire spark application that will be created. It schedules an runs
 * the {@link JobDag}s that are registered in the application, as well as any {@link IJobDagAction}s that are
 * registered.
 *
 * This class is also responsible for the {@link JavaSparkContext}.
 */
@Slf4j
public final class JobManager {

    public static final boolean DEFAULT_LOCK_FREQUENCY = true;

    private static JobManager instance;
    private static final Object lock = new Object();

    @NonNull
    private final Queue<Dag> jobDags = new ConcurrentLinkedDeque<>();
    private final JobDagActions postJobManagerActions;
    @Getter
    private final Configuration conf;

    @NotEmpty
    private final String appName;
    private final String appId;
    private final JobLockManager jobLockManager;

    @Getter
    private final JobMetrics jobMetrics;

    @Getter
    private final Reporters reporters;

    @Getter
    private final SparkFactory sparkFactory;

    @Getter @Setter
    private JobManagerMetadataTracker tracker;

    @Getter @Setter
    private  boolean jobManagerMetadataEnabled;

    @Setter
    private Optional<IJobExecutionStrategy> jobExecutionStrategy = Optional.absent();

    @Getter
    private final JobManagerStatus jobManagerStatus;

    private JobManager(@NonNull final Configuration conf, @NotEmpty final String appName,
        @NotEmpty final String frequency, final boolean shouldLockFrequency,
        @NonNull final SparkFactory sparkFactory, @NonNull final Reporters reporters,
        @NonNull final JobManagerStatus jobManagerStatus) {
        if (sparkFactory.getSparkArgs().getAvroSchemas().isEmpty()) {
            log.warn("Schemas must be added to SparkArgs before creating sparkContext. Found no schemas");
        }
        this.conf = conf;
        this.appName = appName;
        this.jobMetrics = new JobMetrics(appName);
        this.reporters = reporters;
        this.sparkFactory = sparkFactory;
        this.reporters.addReporter(new ConsoleReporter());
        this.jobLockManager = new JobLockManager(conf, frequency, shouldLockFrequency);
        this.postJobManagerActions = new JobDagActions(this.reporters, "jobManager");
        this.appId = sparkFactory.getSparkContext().sc().applicationId();
        this.jobManagerStatus = jobManagerStatus;
    }

    /**
     * Create the JobManager. Will fail if the job manager has already been created.
     * @param conf Configuration for the job manager, used to determine parallelism of execution
     * @param appName Name of the application, used in the SparkContext
     * @param frequency name of the frequency, used to lock entire frequencies
     * @param lockFrequency whether the frequency should be locked
     * @param sparkFactory to provide {@link JavaSparkContext}
     * @param reporters to report metrics
     * @param jobManagerStatus to report status
     */
    public static JobManager createJobManager(@NonNull final Configuration conf,
        @NotEmpty final String appName,
        @NotEmpty final String frequency, final boolean lockFrequency,
        @NonNull final SparkFactory sparkFactory, @NonNull final Reporters reporters,
        @NonNull final JobManagerStatus jobManagerStatus) {
        synchronized (lock) {
            Preconditions.checkState(instance == null,
                    "JobManager was already created");
            instance = new JobManager(conf, appName, frequency, lockFrequency, sparkFactory,
                    reporters, jobManagerStatus);
        }
        return instance;
    }

    /**
     * Create the JobManager. Will fail if the job manager has already been created.
     * @param conf Configuration for the job manager, used to determine parallelism of execution
     * @param appName Name of the application, used in the SparkContext
     * @param frequency name of the frequency, used to lock entire frequencies
     * @param lockFrequency whether the frequency should be locked
     * @param sparkFactory to provide {@link JavaSparkContext}
     * @param reporters to report metrics
     */
    public static JobManager createJobManager(@NonNull final Configuration conf,
        @NotEmpty final String appName,
        @NotEmpty final String frequency, final boolean lockFrequency,
        @NonNull final SparkFactory sparkFactory, @NonNull final Reporters reporters) {
        return createJobManager(conf, appName, frequency, lockFrequency, sparkFactory,
                reporters, new JobManagerStatus());
    }

    /**
     * Create the JobManager. Will fail if the job manager has already been created.
     * @param conf Configuration for the job manager, used to determine parallelism of execution
     * @param appName Name of the application, used in the SparkContext
     * @param frequency name of the frequency, used to lock entire frequencies
     * @param sparkFactory to provide {@link JavaSparkContext}
     * @param reporters to report metrics
     */
    public static JobManager createJobManager(@NonNull final Configuration conf,
        @NotEmpty final String appName,
        @NotEmpty final String frequency, @NonNull final SparkFactory sparkFactory,
        final Reporters reporters) {
        return createJobManager(conf, appName, frequency, DEFAULT_LOCK_FREQUENCY, sparkFactory,
            reporters, new JobManagerStatus());
    }

    /**
     * Execute all registered {@link JobDag}, then perform all registered {@link IJobDagAction}
     */
    public void run() {
        final Queue<Future<Pair<String, IStatus>>> futures = new ConcurrentLinkedDeque<>();
        final AtomicBoolean isSuccess = new AtomicBoolean(true);
        // ensure the SparkContext has been created
        Preconditions.checkState(!this.jobDags.isEmpty(), "No job dags to execute");
        final JavaSparkContext javaSparkContext = sparkFactory.getSparkContext();
        TimeoutManager.init(this.conf, javaSparkContext.sc());
        final boolean hasMultipleDags = this.jobDags.size() > 1;
        final Queue<Dag> runtimeJobDagOrder;
        if (hasMultipleDags && this.jobExecutionStrategy.isPresent()) {
            runtimeJobDagOrder = new ConcurrentLinkedDeque<>(this.jobExecutionStrategy.get().sort(this.jobDags));
        } else {
            runtimeJobDagOrder = this.jobDags;
        }
        try {
            ThreadPoolService.init(this.conf);
            runtimeJobDagOrder.forEach(jobDag ->
                    futures.add(ThreadPoolService.submit(
                        () -> {
                            SparkJobTracker.setJobName(javaSparkContext.sc(), jobDag.getDataFeedName());
                            if (hasMultipleDags) {
                                setSparkStageName(javaSparkContext, jobDag.getDataFeedName());
                            }
                            final IStatus status = jobDag.execute();
                            this.jobManagerStatus.addJobStatus(jobDag.getJobName(), status);
                            return new Pair<>(jobDag.getJobName(), status);
                        }, ThreadPoolServiceTier.JOB_DAG_TIER)));

            TimeoutManager.getInstance().startMonitorThread();
            futures.forEach(future -> {
                    try {
                        final Optional<Pair<String, IStatus>> result = Optional.fromNullable(future.get());
                        IStatus.Status status = result.get().value().getStatus();
                        log.info("job dag, name: {}, status: {}",
                                 result.get().key(), status.name());
                        if (IStatus.Status.FAILURE.equals(status)) {
                            log.error("Unsuccessful run, jobdag: {}", result.get().key());
                            isSuccess.set(false);
                        }
                    } catch (Exception e) {
                        log.error("Error running job", e);
                        isSuccess.set(false);
                        this.jobManagerStatus.setStatus(IStatus.Status.FAILURE);
                        this.jobManagerStatus.addException(e);
                    }
                }
            );
            if (TimeoutManager.getInstance().getTimedOut()) {
                log.error("Time out error while running job.");
                isSuccess.set(false);
            }
            // if we're not reporting success/failure through status, we need to throw an exception on failure
            if (!isSuccess.get()) {
                throw new JobRuntimeException("Error while running job.  Look at previous log entries for detail");
            }
        } catch (final Throwable t) {
            log.error("Failed in JobManager", t);
            isSuccess.set(false);
            this.jobManagerStatus.setStatus(IStatus.Status.FAILURE);
            if (t instanceof Exception) {
                // trap exceptions and add them to the status
                this.jobManagerStatus.addException((Exception) t);
            } else {
                // let errors be thrown
                throw t;
            }
        } finally {
            this.postJobManagerActions.execute(isSuccess.get());
            shutdown(!isSuccess.get());
            this.reporters.getReporters().forEach(IReporter::finish);
        }
    }

    /**
     * Add {@link JobDag} to be executed on {@link #run()}
     * @param jobDag JobDag to be added
     */
    public void addJobDag(@NonNull final Dag jobDag) {
        if (jobLockManager.lockDag(jobDag.getJobName(), jobDag.getDataFeedName())) {
            this.jobDags.add(jobDag);
        } else {
            log.warn("Failed to obtain lock for JobDag {} - {}", jobDag.getJobName(), jobDag.getDataFeedName());
        }
    }

    /**
     * Add collection of {@link JobDag} to be executed on {@link #run()}
     * @param jobDags collection of JobDags to be added
     */
    public void addJobDags(@NonNull final Collection<? extends JobDag> jobDags) {
        jobDags.forEach(this::addJobDag);
    }

    /**
     * Add {@link IJobDagAction} to be executed after all {@link JobDag} have completed
     * @param action action to add
     */
    public void addPostJobManagerAction(@NonNull final IJobDagAction action) {
        this.postJobManagerActions.addAction(action);
    }

    /**
     * Add collection of {@link IJobDagAction} to be executed after all {@link JobDag} have completed
     * @param actions action to add
     */
    public void addPostJobManagerActions(@NonNull final Collection<? extends IJobDagAction> actions) {
        actions.forEach(this::addPostJobManagerAction);
    }

    /* Reset the singleton, shutting down any running spark or threads
     */
    @VisibleForTesting
    public static void reset() {
        if (instance != null) {
            instance.shutdown(true);
            instance = null;
        }
    }

    private void shutdown(final boolean forceShutdown) {
        ThreadPoolService.shutdown(forceShutdown);
        if (this.isJobManagerMetadataEnabled()) {
            this.jobDags.forEach(jobDag -> this.getTracker().set(jobDag.getDataFeedName(),
                jobDag.getJobManagerMetadata()));
            try {
                this.getTracker().writeJobManagerMetadata();
            } catch (MetadataException e) {
                log.error("Unable to save metadata: {}", e.getMessage());
            }
        }
        this.sparkFactory.stop();
        this.jobLockManager.stop();
    }

    private static void setSparkStageName(@NonNull final JavaSparkContext jsc, @NotEmpty final String dataFeedName) {
        // For now we will only set stageName as "dataFeedName" but long term we would want to also include spark's
        // action name in it; which will need support from spark.
        jsc.setCallSite(dataFeedName);
    }

    private final class JobLockManager {
        private static final String MANAGER_LOCK_KEY = "JOBMANAGER";
        private static final String DAG_LOCK_KEY = "JOBDAGS";

        private static final String JOB_FREQUENCY_TAG = "job_frequency";
        private static final String JOB_NAME_TAG = "job_name";
        private static final String DATA_FEED_TAG = "data_feed_name";

        @NonNull
        private final LockManager lockManager;
        @NonNull
        private final String jobFrequency;

        @NonNull
        private final TimerMetric managerTimerMetric;
        @NonNull
        private final HashMap<String, TimerMetric> dagTimerMetricMap;

        private JobLockManager(@NonNull final Configuration conf, @NotEmpty final String frequency,
                final boolean shouldLockFrequency) {
            this.lockManager = new LockManager(conf);
            this.jobFrequency = frequency;

            final String key = LockManager.getLockKey(MANAGER_LOCK_KEY, jobFrequency);
            this.managerTimerMetric = new TimerMetric(JobMetricNames.JOB_MANAGER_LOCK_TIME_MS,
                    ImmutableMap.of(JOB_FREQUENCY_TAG, jobFrequency,
                            JOB_NAME_TAG, appName));

            if (shouldLockFrequency) {
                final boolean success = lockManager.lock(key,
                        String.format("JobManager %s AppId %s", jobFrequency, appId));
                this.managerTimerMetric.stop();
                if (!success) {
                    lockManager.close();
                    throw new IllegalStateException("Failed to obtain lock for JobManager " + jobFrequency);
                }
            } else {
                managerTimerMetric.stop();
                log.info("Frequency lock disabled");
            }
            this.dagTimerMetricMap = new HashMap<>();
        }

        private boolean lockDag(@NotEmpty final String jobDagName, @NotEmpty final String dataFeedName) {
            final String key = LockManager.getLockKey(DAG_LOCK_KEY, jobDagName + "_" + dataFeedName);
            final TimerMetric timerMetric = new TimerMetric(JobMetricNames.JOB_DAG_LOCK_TIME_MS,
                    ImmutableMap.of(
                            JOB_FREQUENCY_TAG, jobFrequency,
                            JOB_NAME_TAG, jobDagName,
                            DATA_FEED_TAG, dataFeedName));
            final boolean success = lockManager.lock(key,
                    String.format("JobDag %s AppId %s", dataFeedName, appId));
            timerMetric.stop();
            dagTimerMetricMap.put(dataFeedName, timerMetric);
            return success;
        }

        private void stop() {
            log.info("Closing the LockManager in the JobManager.");
            this.lockManager.close();
            reporters.report(managerTimerMetric);
            dagTimerMetricMap.forEach((dagName, timerMetric) -> reporters.report(timerMetric));
        }
    }
}
