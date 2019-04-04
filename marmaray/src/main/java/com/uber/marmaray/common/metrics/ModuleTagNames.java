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
package com.uber.marmaray.common.metrics;

        import com.uber.marmaray.common.exceptions.JobRuntimeException;

public final class ModuleTagNames {
    public static final String SOURCE = "source";
    public static final String SINK = "sink";
    public static final String SCHEMA_MANAGER = "schema_manager";
    public static final String SOURCE_CONVERTER = "source_converter";
    public static final String SINK_CONVERTER = "sink_converter";
    public static final String SUB_DAG = "sub_dag";
    public static final String WORK_UNIT_CALCULATOR = "work_unit_calc";
    public static final String JOB_MANAGER = "job_manager";
    public static final String JOB_DAG = "job_dag";
    public static final String METADATA_MANAGER = "metadata_manager";
    public static final String SINK_CONFIGURATION = "sink_configuration";
    public static final String CONFIGURATION = "config";

    private ModuleTagNames() {
        throw new JobRuntimeException("Class should never be instantiated");
    }
}

