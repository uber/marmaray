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
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.exceptions.JobRuntimeException;

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Helper class to manage thread pool service. This is a singleton service and number of threads are controlled by
 * {@link #NUM_THREADS} configuration.
 */
@Slf4j
public final class ThreadPoolService {

    public static final String THREAD_POOL_SERVICE_PREFIX = Configuration.MARMARAY_PREFIX + "thread_pool.";
    public static final String NUM_THREADS = THREAD_POOL_SERVICE_PREFIX + "num_threads";
    public static final String JOB_DAG_THREADS = THREAD_POOL_SERVICE_PREFIX + "job_dag_threads";
    public static final String ACTIONS_THREADS = THREAD_POOL_SERVICE_PREFIX + "actions_threads";

    public static final int DEFAULT_NUM_THREADS = 8;
    public static final int AWAIT_TERMINATION_ATTEMPTS = 30;
    public static final int AWAIT_TERMINATION_CHECK_INTERVAL_MS = 1000;
    public static final int DEFAULT_JOB_DAG_THREADS = 6;
    public static final int DEFAULT_ACTIONS_THREADS = 2;
    public static final long NO_TIMEOUT = -1;
    // Singleton service.
    private static Optional<ThreadPoolService> service = Optional.absent();

    @NonNull
    private final ExecutorService threadPool;

    private final int reservedJobDagThreads;
    private final int reservedActionsThreads;
    private final int numThreads;

    private boolean isShutdown = false;

    private final AtomicInteger currentThreads = new AtomicInteger();
    private final AtomicInteger currentJobDagThreads = new AtomicInteger();
    private final AtomicInteger currentActionsThreads = new AtomicInteger();

    private final Queue<ThreadPoolServiceFuture> jobDagQueue;
    private final Queue<ThreadPoolServiceFuture> actionsQueue;

    private ThreadPoolService(final int numThreads, final int reservedJobDagThreads, final int reservedActionsThreads) {
        log.info("Starting thread pool service numThreads:{} numJobDagThreads:{}", numThreads, reservedJobDagThreads);
        Preconditions.checkState(numThreads > 0 && reservedJobDagThreads > 0 && reservedActionsThreads > 0,
                String.format("Number of threads should be positive: total: %d, jobDag: %d, actions: %d",
                        numThreads, reservedJobDagThreads, reservedActionsThreads));
        Preconditions.checkState(numThreads >= reservedJobDagThreads + reservedActionsThreads,
                String.format(
                        "Total threads must be at least equal to reserved threads: total: %d, jobDag: %d, actions: %d ",
                        numThreads, reservedJobDagThreads, reservedActionsThreads));
        this.reservedActionsThreads = reservedActionsThreads;
        this.reservedJobDagThreads = reservedJobDagThreads;
        this.threadPool = Executors.newFixedThreadPool(numThreads);
        this.actionsQueue = new ConcurrentLinkedDeque<>();
        this.jobDagQueue = new ConcurrentLinkedDeque<>();
        this.numThreads = numThreads;
    }

    /**
     * Initialize the ThreadPoolService
     *
     * @param conf Configuration from which to load the properties.
     */
    public static synchronized void init(@NonNull final Configuration conf) {
        final int numThreads = conf.getIntProperty(NUM_THREADS, DEFAULT_NUM_THREADS);
        final int reservedJobDagThreads = conf.getIntProperty(JOB_DAG_THREADS,
                DEFAULT_JOB_DAG_THREADS);
        final int reservedActionsThreads = conf.getIntProperty(ACTIONS_THREADS,
                DEFAULT_ACTIONS_THREADS);
        if (service.isPresent()) {
            throw new JobRuntimeException("Re-initializing thread pool service.");
        }
        service = Optional.of(new ThreadPoolService(numThreads, reservedJobDagThreads, reservedActionsThreads));
    }

    /**
     * Submit a callable to the pool, in the correct tier.
     *
     * @param callable callable to submit
     * @param tier     tier to submit to
     * @return Future tied to the callable's execution
     */
    public static Future submit(@NonNull final Callable callable, @NonNull final ThreadPoolServiceTier tier) {
        return submit(callable, tier, NO_TIMEOUT);
    }

    /**
     * Submit a callable to the pool, in the correct tier.
     * Callable has a timeout of timeoutSeconds, which starts once the callable has been added to the pool.
     *
     * @param callable       callable to submit
     * @param tier           tier to submit to
     * @param timeoutSeconds timeout in seconds of the callable
     * @return Future tied to the callable's execution
     */
    public static synchronized Future submit(@NonNull final Callable callable,
        @NonNull final ThreadPoolServiceTier tier, final long timeoutSeconds) {
        Preconditions.checkState(service.isPresent(), "No thread pool service is running");
        Preconditions.checkState(!service.get().isShutdown, "Service is shutting down");
        final ThreadPoolService service = ThreadPoolService.service.get();
        final ThreadPoolServiceCallable threadPoolServiceCallable = service
                .new ThreadPoolServiceCallable(callable, tier);
        final ThreadPoolServiceFuture future = service
                .new ThreadPoolServiceFuture(threadPoolServiceCallable, timeoutSeconds,
                        TimeUnit.SECONDS);
        threadPoolServiceCallable.setFuture(future);
        synchronized (service) {
            if (tier.equals(ThreadPoolServiceTier.JOB_DAG_TIER)) {
                if (service.canScheduleJobDag()) {
                    future.addWrappedFuture(service.scheduleJobDag(threadPoolServiceCallable));
                } else {
                    service.queueJobDag(future);
                }
            } else if (tier.equals(ThreadPoolServiceTier.ACTIONS_TIER)) {
                if (service.canScheduleAction()) {
                    future.addWrappedFuture(service.scheduleAction(threadPoolServiceCallable));
                } else {
                    service.queueAction(future);
                }
            } else {
                throw new JobRuntimeException("Trying to submit to illegal tier " + ThreadPoolServiceTier.JOB_DAG_TIER);
            }
        }
        return future;
    }

    /**
     * Check if the service is already initialized
     * @return true if the service is ready to submit
     */
    public static boolean isInitialized() {
        return service.isPresent();
    }

    private void queueAction(final ThreadPoolServiceFuture future) {
        this.actionsQueue.add(future);
    }

    private Future scheduleAction(final Callable<Object> callable) {
        this.currentActionsThreads.incrementAndGet();
        this.currentThreads.incrementAndGet();
        return this.threadPool.submit(callable);
    }

    private boolean canScheduleAction() {
        return (this.currentThreads.get() < this.numThreads
                // if we have fewer than reserved job dag threads, we can schedule
                && (this.currentActionsThreads.get() < this.reservedActionsThreads
                // if we have room to schedule a job dag without reaching into the actions reserve, we can schedule
                || this.currentActionsThreads.get() < this.numThreads - this.reservedJobDagThreads));
    }

    private void queueJobDag(@NonNull final ThreadPoolServiceFuture future) {
        this.jobDagQueue.add(future);
    }

    private <T> Future scheduleJobDag(final Callable<T> callable) {
        this.currentThreads.incrementAndGet();
        this.currentJobDagThreads.incrementAndGet();
        return this.threadPool.submit(callable);
    }

    private boolean canScheduleJobDag() {
        return (this.currentThreads.get() < this.numThreads
                // if we have fewer than reserved job dag threads, we can schedule
                && (this.currentJobDagThreads.get() < this.reservedJobDagThreads
                // if we have room to schedule a job dag without reaching into the actions reserve, we can schedule
                || this.currentJobDagThreads.get() < this.numThreads - this.reservedActionsThreads));
    }

    /**
     * Will wait for all threads to finish their task unless forceShutdown is set to true in which case service will
     * be forcefully shutdown.
     */
    public static void shutdown(final boolean forceShutdown) {
        ThreadPoolService currentService = null;
        synchronized (ThreadPoolService.class) {
            if (!service.isPresent()) {
                return;
            }
            currentService = service.get();
            if (currentService.isShutdown) {
                return;
            }
            log.info("Shutting down thread pool service");
            currentService.isShutdown = true;
            service = Optional.absent();
        }
        if (forceShutdown) {
            log.error("forcefully shutting down waiting threads");
            currentService.threadPool.shutdownNow();
        } else {
            currentService.shutdown();
        }
    }

    private synchronized boolean removeFromQueue(@NonNull final ThreadPoolServiceFuture future) {
        return this.actionsQueue.remove(future) || this.jobDagQueue.remove(future);
    }

    private void shutdown() {
        int terminationChecks = 0;
        while (terminationChecks < AWAIT_TERMINATION_ATTEMPTS && (!this.jobDagQueue.isEmpty()
                || !this.actionsQueue.isEmpty())) {
            log.info("waiting for tasks to clear out of queue.");
            try {
                Thread.sleep(AWAIT_TERMINATION_CHECK_INTERVAL_MS);
                terminationChecks += 1;
            } catch (InterruptedException e) {
                // pass
            }
        }
        this.threadPool.shutdown();
        while (!this.threadPool.isTerminated()) {
            try {
                log.info("waiting for tasks to stop.");
                if (!this.threadPool.awaitTermination(
                        (AWAIT_TERMINATION_ATTEMPTS - terminationChecks) * AWAIT_TERMINATION_CHECK_INTERVAL_MS,
                        TimeUnit.MILLISECONDS)) {
                    this.threadPool.shutdownNow();
                }
            } catch (InterruptedException e) {
                throw new JobRuntimeException("Error waiting for threadpool to stop", e);
            }
        }
    }

    private synchronized void finishExecution(@NonNull final ThreadPoolServiceTier tier) {
        if (tier.equals(ThreadPoolServiceTier.ACTIONS_TIER)) {
            if (this.actionsQueue.isEmpty()) {
                if (this.jobDagQueue.isEmpty() || !canScheduleJobDag()) {
                    // nothing else to run, just reduce number of threads
                    this.currentThreads.decrementAndGet();
                    this.currentActionsThreads.decrementAndGet();
                } else {
                    // can now move a jobDag thread to an actions thread
                    submitFuture(this.jobDagQueue.poll());
                    this.currentActionsThreads.decrementAndGet();
                    this.currentJobDagThreads.incrementAndGet();
                }
            } else {
                submitFuture(this.actionsQueue.poll());
            }
        } else if (tier.equals(ThreadPoolServiceTier.JOB_DAG_TIER)) {
            if (this.jobDagQueue.isEmpty()) {
                if (this.actionsQueue.isEmpty() || !canScheduleAction()) {
                    // nothing else to run, reduce count of running threads
                    this.currentThreads.decrementAndGet();
                    this.currentJobDagThreads.decrementAndGet();
                } else {
                    // can now move an actions thread to a jobDag thread
                    submitFuture(this.actionsQueue.poll());
                    this.currentJobDagThreads.decrementAndGet();
                    this.currentActionsThreads.incrementAndGet();
                }
            } else {
                submitFuture(this.jobDagQueue.poll());
            }
        } else {
            throw new JobRuntimeException(String.format("Attempting to finish illegal tier %s", tier.toString()));
        }
    }

    private void submitFuture(@NonNull final ThreadPoolServiceFuture future) {
        future.addWrappedFuture(this.threadPool.submit(future.getWrappedCallable()));
    }

    private final class ThreadPoolServiceFuture implements Future {

        @Getter
        private final Callable wrappedCallable;
        private Future wrappedFuture = null;
        private LinkedBlockingQueue<Future> wrappedFutureWaitQ = new LinkedBlockingQueue<>();
        private final long timeout;
        private final TimeUnit timeUnit;
        private boolean cancelled = false;
        private boolean done = false;

        private ThreadPoolServiceFuture(@NonNull final Callable wrappedCallable, final long timeout,
                @NonNull final TimeUnit timeUnit) {
            this.wrappedCallable = wrappedCallable;
            this.timeout = timeout;
            this.timeUnit = timeUnit;
        }

        public void addWrappedFuture(@NonNull final Future wrappedFuture) {
            this.wrappedFutureWaitQ.offer(wrappedFuture);
        }

        private void waitForWrappedFuture(final long timeout,
            @NonNull final TimeUnit timeUnit) throws TimeoutException {
            if (this.wrappedFuture == null) {
                try {
                    this.wrappedFuture = this.wrappedFutureWaitQ.poll(timeout, timeUnit);
                    if (this.wrappedFuture == null) {
                        throw new TimeoutException("no wrapped future received");
                    }
                } catch (InterruptedException e) {
                    throw new JobRuntimeException(e);
                }
            }
        }

        private void waitForWrappedFuture() {
            if (this.wrappedFuture == null) {
                try {
                    this.wrappedFuture = this.wrappedFutureWaitQ.take();
                } catch (InterruptedException e) {
                    throw new JobRuntimeException(e);
                }
            }
        }

        @Override
        public boolean cancel(final boolean mayInterruptIfRunning) {
            this.cancelled = removeFromQueue(this);
            if (!this.wrappedFutureWaitQ.isEmpty()) {
                waitForWrappedFuture();
            }
            if (!this.cancelled && this.wrappedFuture != null) {
                this.cancelled = this.wrappedFuture.cancel(mayInterruptIfRunning);
            }
            return this.cancelled;
        }

        @Override
        public boolean isCancelled() {
            return this.cancelled;
        }

        /**
         * @return true if the operation finished (with or without error) or it was cancelled else false.
         */
        @Override
        public boolean isDone() {
            return this.done || this.cancelled;
        }

        @Override
        public Object get() throws InterruptedException, ExecutionException {
            waitForWrappedFuture();
            if (this.timeout != ThreadPoolService.NO_TIMEOUT) {
                try {
                    return this.wrappedFuture.get(this.timeout, this.timeUnit);
                } catch (TimeoutException e) {
                    throw new JobRuntimeException(e);
                }
            } else {
                return this.wrappedFuture.get();
            }
        }

        @Override
        public Object get(final long timeout, @NonNull final TimeUnit timeUnit)
                throws InterruptedException, ExecutionException, TimeoutException {
            final long endTimeMs = System.currentTimeMillis() + timeUnit.toMillis(timeout);
            waitForWrappedFuture(timeout, timeUnit);
            return this.wrappedFuture.get(Math.max(1, endTimeMs - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
        }
    }

    private final class ThreadPoolServiceCallable implements Callable {

        private final Callable wrappedCallable;
        private final ThreadPoolServiceTier tier;
        private ThreadPoolServiceFuture future;

        private ThreadPoolServiceCallable(@NonNull final Callable wrappedCallable,
                @NonNull final ThreadPoolServiceTier tier) {
            this.wrappedCallable = wrappedCallable;
            this.tier = tier;
        }

        @Override
        public Object call() throws Exception {
            try {
                final Object result = this.wrappedCallable.call();
                return result;
            } finally {
                finishExecution(this.tier);
                this.future.done = true;
            }
        }

        private void setFuture(@NonNull final ThreadPoolServiceFuture future) {
            this.future = future;
        }
    }
}
