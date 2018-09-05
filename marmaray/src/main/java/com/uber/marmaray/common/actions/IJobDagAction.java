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

import java.util.HashMap;
import java.util.Map;

/**
 * {@link IJobDagAction} is interface to determine a generic action to execute.
 * {@link JobDagActions} are completely independent and will determine if they should run based on success status.
 */
public interface IJobDagAction {
    int DEFAULT_TIMEOUT_SECONDS = 120;
    String ACTION_TYPE = "action_type";

    /**
     * Execute the action
     *
     * @param successful whether the job dag succeeded
     * @return true if action succeeded
     */
    boolean execute(boolean successful);

    /**
     * Timeout to wait for the action to complete
     * @return number of seconds to wait for task completion
     */
    default int getTimeoutSeconds() {
        return DEFAULT_TIMEOUT_SECONDS;
    }

    /**
     * @return metric tags to be used for reporting metrics.
     */
    default Map<String, String> getMetricTags() {
        final Map<String, String> metricsTags = new HashMap<>();
        metricsTags.put(ACTION_TYPE, this.getClass().getSimpleName());
        return metricsTags;
    }
}
