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
package com.uber.marmaray.common.util;

import com.uber.marmaray.common.configuration.CassandraSinkConfiguration;
import com.uber.marmaray.common.configuration.Configuration;

public class CassandraTestConstants {
    public static final String KEY_SPACE = "marmaray";
    public static final String TABLE = "crossfit_gyms";
    public static final String LOCALHOST = "localhost";
    public static final String INT_FIELD = "int_field";
    public static final String STRING_FIELD = "string_field";
    public static final String BOOLEAN_FIELD = "boolean_field";
    public static final int CASSANDRA_PORT = 9142;
    public static final Configuration CONFIGURATION = new Configuration();
    static {
        CONFIGURATION.setProperty(CassandraSinkConfiguration.KEYSPACE, KEY_SPACE);
        CONFIGURATION.setProperty(CassandraSinkConfiguration.TABLE_NAME, TABLE);
        CONFIGURATION.setProperty(CassandraSinkConfiguration.CLUSTER_NAME, "test-cluster");
        CONFIGURATION.setProperty(CassandraSinkConfiguration.PARTITION_KEYS, "key_name1,key_name2");
    }
}
