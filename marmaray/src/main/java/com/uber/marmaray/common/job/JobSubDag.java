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
import com.google.common.base.Preconditions;
import com.uber.marmaray.common.IPayload;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.metrics.DataFeedMetrics;
import com.uber.marmaray.common.metrics.IMetricable;
import com.uber.marmaray.common.metrics.JobMetrics;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.validator.constraints.NotEmpty;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * {@link JobSubDag} is useful when we need to run custom job dag which can be represented in the form of an n-way
 * tree. User should implement {@link #executeNode(Optional)}, {@link #commitNode()} and
 * {@link #getDataForChild(JobSubDag)} for every node in the jobDag. The execution sequence is as follows.
 * Consider an example Dag.
 * # Note :- Numbers in the bracket denote their priority. Priority is only compared between children of same parent
 * # node.
 * #              N1
 * #             /  \
 * #            /    \
 * #         (0)N2  (1)N3
 * #          /
 * #         /
 * #      (0)N4
 * Caller should call below methods for complete execution of the above dag. [N1.execute() and N1.commit()].
 * This is how the job dag will get executed.
 * 1) N1.execute() <- user calls this.
 * 1-a) N1.executeNode() [Step1]
 * 2-a) N2.execute() [Step2]
 * 2-a-a) N2.executeNode() [Step3]
 * 2-a-b) N4.execute() [Step4]
 * 2-a-b-a) N4.executeNode() [Step5]
 * 2-a) N3.execute() [Step2]
 * 2-a-a) N3.executeNode() [Step3]
 * // It will wait for all tasks to finish.
 * 2) N1.commit() <- user calls this.
 * 2-a) N2.commit() [Step1]
 * 2-a-a) N4.commit() [Step2]
 * 2-a-b) N4.commitNode() [Step3]
 * 2-a-c) N2.commitNode() [Step4]
 * 2-b) N3.commit() [Step5]
 * 2-b-a) N3.commitNode() [Step6]
 * 2-c) N1.commitNode() [Step7]
 */
@Slf4j
public abstract class JobSubDag implements IMetricable {

    @NotEmpty
    @Getter
    // Logical name of the job node.
    private final String name;

    @Getter
    private Optional<JobMetrics> jobMetrics = Optional.absent();

    @Getter
    private Optional<DataFeedMetrics> dataFeedMetrics = Optional.absent();

    /**
     * Holds child nodes with their priority. ChildNodes are committed from lower to higher priority. All nodes at the
     * same priority are committed in parallel (where ordering is not guaranteed). However
     * {@link #executeNode(Optional)} method for all child nodes is called in parallel.
     */
    private final TreeMap<Integer, List<JobSubDag>> childNodes = new TreeMap<>();

    protected JobSubDag(@NotEmpty final String name) {
        this.name = name;
    }

    public void addSubDag(final int priority, @NonNull final JobSubDag subDag) {
        if (!this.childNodes.containsKey(priority)) {
            this.childNodes.put(priority, new LinkedList<>());
        }
        this.childNodes.get(priority).add(subDag);
    }

    @Override
    public void setJobMetrics(@NonNull final JobMetrics jobMetrics) {
        this.jobMetrics = Optional.of(jobMetrics);
    }

    @Override
    public void setDataFeedMetrics(@NonNull final DataFeedMetrics dataFeedMetrics) {
        this.dataFeedMetrics = Optional.of(dataFeedMetrics);
    }

    /**
     * Should pre process input and create appropriate {@link IPayload} for child nodes. Later when childNode's
     * {@link #execute(Optional)} method is called it will call {@link #getDataForChild(JobSubDag)} to retrieve payload
     * for it.
     *
     * @param data input payload
     */
    protected abstract void executeNode(@NonNull final Optional<IPayload> data);

    /**
     * An optional method which user can implement. Ideally this should be used to call atomic operations.
     */
    protected void commitNode() {
    }

    /**
     * Called to retrieve payload for child dag.
     *
     * @param childSubDag childSubDag for which {@link IPayload} data needs to be retrieved
     * @return payload for child dag
     */
    protected Optional<IPayload> getDataForChild(@NonNull final JobSubDag childSubDag) {
        return Optional.absent();
    }

    /**
     * Will execute current node's {@link #executeNode(Optional)} followed by parallel execution of {@link
     * #childNodes}'s {@link #execute(Optional)} method.
     */
    public final void execute(@NonNull final Optional<IPayload> data) {
        Preconditions.checkState(this.dataFeedMetrics.isPresent() && this.jobMetrics.isPresent(),
            "Missing dataFeed or job metrics");
        log.info("running : executeNode {}", this.name);
        // first call Current node's executeNode().
        executeNode(data);

        // setup metrics for child nodes.
        setupChildMetrics();

        final Queue<SubDagExecutionStatus> statuses = new LinkedList<>();
        this.childNodes.entrySet().stream().forEach(
            childNodesAtSamePriority -> {
                childNodesAtSamePriority.getValue().stream().forEach(
                    childNode -> statuses.add(
                        new SubDagExecutionStatus(childNode,
                            ThreadPoolService.submit(
                                () -> {
                                    childNode.execute(getDataForChild(childNode));
                                    return 0;
                                }, ThreadPoolServiceTier.JOB_DAG_TIER
                            )))
                );
            }
        );
        waitForTasksToFinish(statuses);
    }

    private void setupChildMetrics() {

        this.childNodes.entrySet().stream().forEach(
            entry -> entry.getValue().stream().forEach(
                jobSubDag -> {
                    jobSubDag.setJobMetrics(this.jobMetrics.get());
                    jobSubDag.setDataFeedMetrics(this.dataFeedMetrics.get());
                }
            ));
    }

    // Helper method to wait for parallel tasks to finish execution.
    private void waitForTasksToFinish(@NonNull final Queue<SubDagExecutionStatus> statuses) {
        while (!statuses.isEmpty()) {
            final SubDagExecutionStatus status = statuses.poll();
            while (!status.getStatus().isDone()) {
                try {
                    status.getStatus().get();
                } catch (InterruptedException e) {
                    log.error("interrupted {} {}", status.getSubDag().getName(), e);
                    throw new JobRuntimeException("dag execution interrupted", e);
                } catch (ExecutionException e) {
                    log.error("failed to execute subdag {} {}", status.getSubDag().getName(), e.getCause());
                    throw new JobRuntimeException("failed to execute subDag", e.getCause());
                }
            }
        }
    }

    /**
     * Will execute childNode's {@link #commitNode()} method from lower priority to higher priority.
     * {@link #commitNode()} method of the nodes at same priority level will get executed in parallel.
     */
    public final void commit() {
        log.info("calling {}'s childNodes' commit", this.name);
        this.childNodes.entrySet().stream().forEach(
            childNodesAtSamePriority -> {
                final Queue<SubDagExecutionStatus> statuses = new LinkedList<>();
                childNodesAtSamePriority.getValue().stream().forEach(
                    childNode -> statuses.add(
                        new SubDagExecutionStatus(childNode,
                            ThreadPoolService.submit(
                                () -> {
                                    childNode.commit();
                                    return 0;
                                }, ThreadPoolServiceTier.JOB_DAG_TIER
                            )))
                );
                waitForTasksToFinish(statuses);
            }
        );
        log.info("calling {}'s commitNode", this.name);
        commitNode();
    }

    /**
     * Helper class to wrap {@link JobSubDag} with it's run status.
     */
    @Data
    class SubDagExecutionStatus {

        private final JobSubDag subDag;
        private final Future<Integer> status;
    }
}
