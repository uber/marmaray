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
package com.uber.marmaray.common.configuration;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.uber.marmaray.utilities.StringTypes;
import com.uber.marmaray.utilities.ConfigUtil;
import com.google.common.collect.Lists;

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * {@link CassandraMetadataManagerConfiguration} contains all the generic metadata information for where Hive is either a source or
 * sink for the data pipeline job.  All CassandraMetadataManagerConfiguration properties starts with {@link #CASSANDRA_METADATA_MANAGER_PREFIX}.
 */

@Slf4j
public class CassandraMetadataManagerConfiguration extends MetadataManagerConfiguration {
    public static final String CASSANDRA_METADATA_MANAGER_PREFIX = METADATA_MANAGER_PREFIX + "cassandra.";
    public static final String USERNAME = CASSANDRA_METADATA_MANAGER_PREFIX + "username";
    public static final String PASSWORD = CASSANDRA_METADATA_MANAGER_PREFIX + "password";
    public static final String KEYSPACE = CASSANDRA_METADATA_MANAGER_PREFIX + "keyspace";
    public static final String TABLE_NAME = CASSANDRA_METADATA_MANAGER_PREFIX + "table_name";
    public static final String CLUSTER = CASSANDRA_METADATA_MANAGER_PREFIX + "cluster";

    // cassandra setting
    public static final String DC_PATH = CASSANDRA_METADATA_MANAGER_PREFIX + "data_center_path";
    public static final String DATACENTER = CASSANDRA_METADATA_MANAGER_PREFIX + "datacenter";
    public static final String NATIVE_TRANSPORT_PORT = CASSANDRA_METADATA_MANAGER_PREFIX + "output.native.port";
    public static final String INITIAL_HOSTS = CASSANDRA_METADATA_MANAGER_PREFIX + "output.thrift.address";
    public static final String DEFAULT_OUTPUT_NATIVE_PORT = "9042";
    public static final String OUTPUT_THRIFT_PORT = CASSANDRA_METADATA_MANAGER_PREFIX + "output.thrift.port";
    public static final String STORAGE_PORT = CASSANDRA_METADATA_MANAGER_PREFIX + "storage.port";
    public static final String SSL_STORAGE_PORT = CASSANDRA_METADATA_MANAGER_PREFIX + "ssl.storage.port";
    public static final String DISABLE_QUERY_UNS = CASSANDRA_METADATA_MANAGER_PREFIX + "disable_uns";

    private static final Splitter splitter = Splitter.on(StringTypes.COMMA);

    @Getter
    protected final List<String> initialHosts;

    @Getter
    private final String username;

    @Getter
    private final String password;

    @Getter
    private final String keyspace;

    @Getter
    private final String tableName;

    @Getter
    private final String cluster;

    public CassandraMetadataManagerConfiguration(@NonNull final Configuration conf) {
        super(conf);
        ConfigUtil.checkMandatoryProperties(conf, this.getMandatoryProperties());

        this.username = conf.getProperty(USERNAME).get();
        this.password =  conf.getProperty(PASSWORD).get();
        this.keyspace = conf.getProperty(KEYSPACE).get();
        this.tableName = conf.getProperty(TABLE_NAME).get();
        this.cluster = conf.getProperty(CLUSTER).get();

        if (conf.getProperty(INITIAL_HOSTS).isPresent()) {
            this.initialHosts = splitString(conf.getProperty(INITIAL_HOSTS).get());
        } else {
            this.initialHosts = new ArrayList<>();
        }
    }

    public Optional<String> getNativePort() {
        return getConf().getProperty(NATIVE_TRANSPORT_PORT);
    }

    protected List<String> splitString(final String commaSeparatedValues) {
        return Lists.newArrayList(splitter.split(commaSeparatedValues));
    }

    public static List<String> getMandatoryProperties() {
        return Collections.unmodifiableList(
                Arrays.asList(
                        KEYSPACE,
                        CLUSTER,
                        TABLE_NAME
                ));
    }
}
