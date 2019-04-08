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
import com.uber.marmaray.common.AvroPayload;
import com.uber.marmaray.common.actions.IJobDagAction;
import com.uber.marmaray.common.actions.JobDagActions;
import com.uber.marmaray.common.actions.ReporterAction;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.metadata.AbstractValue;
import com.uber.marmaray.common.metadata.IMetadataManager;
import com.uber.marmaray.common.metrics.DataFeedMetricNames;
import com.uber.marmaray.common.metrics.DataFeedMetrics;
import com.uber.marmaray.common.metrics.ErrorCauseTagNames;
import com.uber.marmaray.common.metrics.JobMetrics;
import com.uber.marmaray.common.metrics.LongMetric;
import com.uber.marmaray.common.metrics.ModuleTagNames;
import com.uber.marmaray.common.metrics.TimerMetric;
import com.uber.marmaray.common.reporters.Reporters;
import com.uber.marmaray.common.sinks.ISink;
import com.uber.marmaray.common.sources.IRunState;
import com.uber.marmaray.common.sources.ISource;
import com.uber.marmaray.common.sources.IWorkUnitCalculator;
import com.uber.marmaray.common.sources.IWorkUnitCalculator.IWorkUnitCalculatorResult;
import com.uber.marmaray.common.status.BaseStatus;
import com.uber.marmaray.common.status.IStatus;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.hibernate.validator.constraints.NotEmpty;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class JobDag<T, V extends AbstractValue, R extends IRunState<R>, C extends IWorkUnitCalculator<T, R, K, V>,
    K extends IWorkUnitCalculatorResult<T, R>> extends Dag {

    public static final String LAST_RUNTIME_METADATA_KEY = "runtime";
    public static final String LAST_EXECUTION_METADATA_KEY = "last_execution";
    @NonNull
    private final ISource<K, R> source;
    @NonNull
    private final JobSubDag sinkDag;
    @NonNull
    private final IMetadataManager<V> metadataManager;
    @NonNull
    private final IWorkUnitCalculator<T, R, K, V> workUnitCalculator;

    private final Reporters reporters;
    private final JobDagActions postJobDagActions;

    @NonNull
    private final JobMetrics jobMetrics;
    @Getter
    private final DataFeedMetrics dataFeedMetrics;

    public JobDag(@NonNull final ISource<K, R> source,
                  @NonNull final ISink sink,
                  @NonNull final IMetadataManager<V> metadataManager,
                  @NonNull final IWorkUnitCalculator<T, R, K, V> workUnitCalculator,
                  @NotEmpty final String jobName,
                  @NotEmpty final String dataFeedName,
                  @NonNull final JobMetrics jobMetrics,
                  @NonNull final Reporters reporters) {
        this(source, new SingleSinkSubDag(sink), metadataManager,
                workUnitCalculator, jobName, dataFeedName, jobMetrics, reporters);
    }

    public JobDag(@NonNull final ISource<K, R> source,
                  @NonNull final JobSubDag sinkDag,
                  @NonNull final IMetadataManager<V> metadataManager,
                  @NonNull final IWorkUnitCalculator<T, R, K, V> workUnitCalculator,
                  @NotEmpty final String jobName,
                  @NotEmpty final String dataFeedName,
                  @NonNull final JobMetrics jobMetrics,
                  @NonNull final Reporters reporters) {
        super(jobName, dataFeedName);
        this.source = source;
        this.sinkDag = sinkDag;
        this.metadataManager = metadataManager;
        this.workUnitCalculator = workUnitCalculator;
        this.reporters = reporters;
        this.postJobDagActions = new JobDagActions(this.reporters, dataFeedName);
        this.jobMetrics = jobMetrics;
        this.dataFeedMetrics = new DataFeedMetrics(this.getJobName(),
                Collections.singletonMap(DataFeedMetrics.DATA_FEED_NAME, this.getDataFeedName()));
    }

    // passing datafeed metric from high level job
    public JobDag(@NonNull final ISource<K, R> source,
                  @NonNull final ISink sink,
                  @NonNull final IMetadataManager<V> metadataManager,
                  @NonNull final IWorkUnitCalculator<T, R, K, V> workUnitCalculator,
                  @NotEmpty final String jobName,
                  @NotEmpty final String dataFeedName,
                  @NonNull final JobMetrics jobMetrics,
                  @NonNull final DataFeedMetrics dataFeedMetrics,
                  @NonNull final Reporters reporters) {
        super(jobName, dataFeedName);
        this.source = source;
        this.sinkDag = new SingleSinkSubDag(sink);
        this.metadataManager = metadataManager;
        this.workUnitCalculator = workUnitCalculator;
        this.reporters = reporters;
        this.postJobDagActions = new JobDagActions(this.reporters, dataFeedName);
        this.jobMetrics = jobMetrics;
        this.dataFeedMetrics = dataFeedMetrics;
    }

    /**
     * Add an action on success
     * @param action to execute after job is successful
     */
    public void addAction(final IJobDagAction action) {
        this.postJobDagActions.addAction(action);
    }

    @Override
    public IStatus execute() {

        log.info("Starting job dag for {}", this.getJobName());
        final AtomicBoolean successful = new AtomicBoolean(true);
        final ReporterAction reporterAction = new ReporterAction(this.reporters, this.jobMetrics, this.dataFeedMetrics);
        this.addAction(reporterAction);
        final TimerMetric timerMetric = this.dataFeedMetrics.createTimerMetric(
                DataFeedMetricNames.TOTAL_LATENCY_MS, new HashMap<>(), Optional.absent());
        final BaseStatus status = new BaseStatus();

        try {
            // set up metrics for down streams
            Arrays.asList(this.workUnitCalculator, this.sinkDag, this.source, this.metadataManager)
                    .forEach(metricable -> {
                            metricable.setDataFeedMetrics(this.dataFeedMetrics);
                            metricable.setJobMetrics(this.jobMetrics);
                        });
            // initialize previous run state.
            this.workUnitCalculator.initPreviousRunState(this.metadataManager);
            // compute work units.
            final K workUnitCalculatorResult = this.workUnitCalculator.computeWorkUnits();
            final IStatus workUnitCalculatorResultStatus = workUnitCalculatorResult.getStatus();
            status.mergeStatus(workUnitCalculatorResultStatus);
            log.info("Work unit calculator result :{}", workUnitCalculatorResult);
            // save run state for next processing
            this.workUnitCalculator.saveNextRunState(this.metadataManager, workUnitCalculatorResult.getNextRunState());
            if (workUnitCalculatorResult.hasWorkUnits()) {
                // read source rdd.
                final JavaRDD<AvroPayload> sourceRDD = this.source.getData(workUnitCalculatorResult);

                // execute sink dag.
                this.sinkDag.execute(Optional.of(new DagPayload(sourceRDD)));

                // commit sink dag
                this.sinkDag.commit();
            }

            try {
                this.metadataManager.saveChanges();
            } catch (IOException e) {
                this.dataFeedMetrics.createLongFailureMetric(DataFeedMetricNames.MARMARAY_JOB_ERROR, 1,
                        DataFeedMetricNames.getErrorModuleCauseTags(
                                ModuleTagNames.JOB_DAG, ErrorCauseTagNames.SAVE_METADATA));
                final String msg = "Failed to save metadata changes " + e.getMessage();
                log.error(msg, e);
                throw new JobRuntimeException(msg, e);
            }
        } catch (Exception e) {
            log.error("Failed in JobDag", e);
            this.dataFeedMetrics.createLongFailureMetric(DataFeedMetricNames.MARMARAY_JOB_ERROR, 1,
                    DataFeedMetricNames.getErrorModuleCauseTags(
                            ModuleTagNames.JOB_DAG, ErrorCauseTagNames.ERROR));
            // set failure status if there was an error
            successful.set(false);
            status.setStatus(IStatus.Status.FAILURE);
            status.addException(e);
        } finally {
            // execute all actions at the last minute
            timerMetric.stop();
            reportStatus(successful.get());
            if (getJobManagerMetadata() != null && successful.get()) {
                this.getJobManagerMetadata().put(LAST_RUNTIME_METADATA_KEY, timerMetric.getMetricValue().toString());
                this.getJobManagerMetadata().put(LAST_EXECUTION_METADATA_KEY,
                        String.valueOf(TimeUnit.SECONDS.toMillis(timerMetric.getStartTime().getEpochSecond())));
            }
            this.dataFeedMetrics.createLongMetric(DataFeedMetricNames.RESULT,
                successful.get() ? DataFeedMetricNames.RESULT_SUCCESS : DataFeedMetricNames.RESULT_FAILURE,
                Collections.emptyMap());
            this.postJobDagActions.execute(successful.get());
        }
        return status;
    }

    private void reportStatus(final boolean successful) {
        final long statusValue =
                successful ? DataFeedMetricNames.RESULT_SUCCESS : DataFeedMetricNames.RESULT_FAILURE;
        final LongMetric successMetric = new LongMetric(
                DataFeedMetricNames.RESULT, statusValue);
        successMetric.addTags(this.dataFeedMetrics.getBaseTags());
        this.reporters.report(successMetric);
    }
}
