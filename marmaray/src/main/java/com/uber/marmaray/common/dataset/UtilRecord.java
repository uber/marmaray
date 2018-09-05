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
package com.uber.marmaray.common.dataset;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

/**
 * {@link UtilRecord} is the member type of {@link UtilTable} collections.
 * Subclasses of {@link UtilRecord} must conform to the requirements of a
 * simple Java Bean so they can be converted to {@link org.apache.spark.sql.Dataset},
 * which are:
 * 1) Have primitive field types
 * 2) Have default values for instance fields
 * 3) Have getter and setters for all fields
 * 4) Have a constructor with no arguments
 */
@AllArgsConstructor
@Data
public abstract class UtilRecord implements Serializable {
    private String application_id;
    private String job_name;
    private long job_start_timestamp;
    private long timestamp;
}
