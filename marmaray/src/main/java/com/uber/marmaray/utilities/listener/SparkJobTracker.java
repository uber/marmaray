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

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.StageInfo;
import org.hibernate.validator.constraints.NotEmpty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@link SparkJobTracker} takes a list of registered methods (via {@link SparkJobTracker#registerMethod}),
 * and records the total running time of spark stages and the max running time of a single
 * task in each method.
 */
@Slf4j
public class SparkJobTracker {

    public static final String JOB_NAME_PROP = "marmaray.job_name_prop";

    private static final String UNKNOWN_JOB_NAME = "unknown";

    @Getter
    private static final Map<String, JobRunTimeManager> jobNameMap = new HashMap<>();
    private static final Map<Integer, String> stageIdNameMap = new ConcurrentHashMap<>();

    // To register method, the className needs to be the full class name with package information.
    // Example: SparkJobTracker.registerMethod(MyClass.class.getCanonicalName(), "myFunc");
    public static void registerMethod(@NotEmpty final String className,
                                      @NotEmpty final String methodName) {
        MethodManager.registerMethod(className, methodName);
    }

    /**
     * Sets job name in spark context's local properties. It is used for identifying jobName of running spark DAG.
     * @param sc {@link SparkContext}
     * @param jobName logical job name. for example kafka-topic-name
     */
    public static void setJobName(@NonNull final SparkContext sc, @NotEmpty final String jobName) {
        sc.setLocalProperty(SparkJobTracker.JOB_NAME_PROP, jobName);
    }

    /**
     * @param sc {@link SparkContext}
     * @return job name set using {@link #setJobName(SparkContext, String)} else {@link #UNKNOWN_JOB_NAME}.
     */
    public static String getJobName(@NonNull final SparkContext sc) {
        return getJobName(sc.getLocalProperties());
    }

    private static String getJobName(@NonNull final Properties sparkContextProperties) {
        final String jobName = sparkContextProperties.getProperty(SparkJobTracker.JOB_NAME_PROP);
        return jobName == null ? UNKNOWN_JOB_NAME : jobName;
    }

    public static void logResult() {
        jobNameMap.forEach((name, jobRunTimeManager) -> jobRunTimeManager.logResult());
    }

    public static void logResult(@NotEmpty final String name) {
        if (jobNameMap.containsKey(name)) {
            jobNameMap.get(name).logResult();
        } else {
            log.error("Job Name {} is not found in SparkJobTracker.", name);
        }
    }

    public static void recordStageInfo(@NonNull final StageInfo stageInfo, @NonNull final Properties properties) {
        // record all registered methods related to the stage
        MethodManager.recordStageInfo(stageInfo);

        // add the stageId-jobName mapping
        final String jobName = properties.getProperty(JOB_NAME_PROP, UNKNOWN_JOB_NAME);
        stageIdNameMap.put(stageInfo.stageId(), jobName);
        if (jobName.equals(UNKNOWN_JOB_NAME)) {
            log.warn("Stage Id #{} has an unknown Job Name.", stageInfo.stageId());
        } else {
            log.debug("Stage #{} has a JobName {}.", stageInfo.stageId(), jobName);
        }
    }

    public static void removeStageInfo(@NonNull final StageInfo stageInfo) {
        MethodManager.removeStageInfo(stageInfo);
    }

    public static void recordStageTime(@NonNull final StageInfo stageInfo, final long timeInMs) {
        final String jobName = getJobName(stageInfo.stageId());
        final JobRunTimeManager jobRunTimeManager = getJobRunTimeManager(jobName);
        final List<String> methods = MethodManager.getRelatedMethods(stageInfo.stageId());
        methods.forEach(method -> jobRunTimeManager.reportStageTime(method, timeInMs));
    }

    public static void recordTaskTime(final int stageId, final long timeInMs) {
        final String jobName = getJobName(stageId);
        final JobRunTimeManager jobRunTimeManager = getJobRunTimeManager(jobName);
        final List<String> methods = MethodManager.getRelatedMethods(stageId);
        log.debug("Recording task_time={}ms for StageId={} in methods {}",
                timeInMs, stageId, methods.toString());
        methods.forEach(method -> jobRunTimeManager.reportTaskTime(method, timeInMs));
    }

    private static String getJobName(final int stageId) {
        final String jobName = stageIdNameMap.getOrDefault(stageId, UNKNOWN_JOB_NAME);
        return jobName;
    }

    private static JobRunTimeManager getJobRunTimeManager(@NonNull final String jobName) {
        if (!jobNameMap.containsKey(jobName)) {
            jobNameMap.putIfAbsent(jobName, new JobRunTimeManager(jobName));
        }
        return jobNameMap.get(jobName);
    }

    public static final class JobRunTimeManager {
        private final String jobName;
        @Getter
        private final Map<String, Long> methodTime;
        @Getter
        private final Map<String, Long> maxTaskTime;

        public JobRunTimeManager(@NotEmpty final String jobName) {
            this.jobName = jobName;
            methodTime = new ConcurrentHashMap<>();
            maxTaskTime = new ConcurrentHashMap<>();
        }

        public void reportStageTime(@NonNull final String method, final long timeInMs) {
            final long oldTotal = methodTime.getOrDefault(method, 0L);
            methodTime.put(method, timeInMs + oldTotal);
        }

        public void reportTaskTime(@NonNull final String method, final long timeInMs) {
            final long oldMax = maxTaskTime.getOrDefault(method, 0L);
            maxTaskTime.put(method, Math.max(oldMax, timeInMs));
        }

        public void logResult() {
            methodTime.forEach((method, time) ->
                    log.info("{} - Method {} takes {} ms in total, the max task time is {}",
                            jobName, method, time, maxTaskTime.get(method)));
        }
    }

    private static class MethodManager {
        private static final Map<String, String> registeredMethods = new ConcurrentHashMap<>();

        private static final Map<Integer, List<String>> stageIdRelatedMethodMap = new ConcurrentHashMap<>();

        public static void registerMethod(@NotEmpty final String className,
                                          @NotEmpty final String methodName) {
            final String name = String.format("%s.%s", className, methodName);
            registeredMethods.putIfAbsent(name, "");
            log.info("Method {} registered for SparkListener.", name);
        }

        public static void recordStageInfo(@NonNull final StageInfo stageInfo) {
            final int stageId = stageInfo.stageId();
            if (!stageIdRelatedMethodMap.containsKey(stageId)) {
                stageIdRelatedMethodMap.putIfAbsent(stageId, getRelatedMethods(stageInfo));
            }
        }

        public static List<String> getRelatedMethods(final int stageId) {
            if (!stageIdRelatedMethodMap.containsKey(stageId)) {
                log.warn("StageId {} not recorded.", stageId);
                return new LinkedList<>();
            } else {
                return stageIdRelatedMethodMap.get(stageId);
            }
        }

        public static void removeStageInfo(@NonNull final StageInfo stageInfo) {
            final int stageId = stageInfo.stageId();
            if (!stageIdRelatedMethodMap.containsKey(stageId)) {
                log.warn("Cannot find StageId {} for removeStageInfo.", stageId);
            } else {
                stageIdRelatedMethodMap.remove(stageId);
            }
        }

        private static List<String> parse(final String details) {
            final List<String> stack = new ArrayList<>();
            for (String str: Arrays.asList(details.split("\n"))) {
                stack.add(str.replaceFirst("\\([a-zA-Z0-9:.\\s]*\\)", ""));
            }
            return stack;
        }

        private static List<String> getRelatedMethods(@NonNull final StageInfo stageInfo) {
            if (registeredMethods.isEmpty()) {
                return Collections.singletonList(stageInfo.name());
            } else {
                final List<String> stack = parse(stageInfo.details());
                stack.removeIf(s -> !registeredMethods.containsKey(s));
                return stack;
            }
        }
    }
}
