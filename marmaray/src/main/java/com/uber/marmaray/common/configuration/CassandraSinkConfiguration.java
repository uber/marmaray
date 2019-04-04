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
import com.google.common.collect.Lists;
import com.uber.marmaray.common.PartitionType;
import com.uber.marmaray.common.exceptions.MissingPropertyException;
import com.uber.marmaray.common.metrics.DataFeedMetrics;
import com.uber.marmaray.common.metrics.IMetricable;
import com.uber.marmaray.common.metrics.JobMetrics;
import com.uber.marmaray.common.schema.cassandra.ClusterKey;
import com.uber.marmaray.utilities.ConfigUtil;
import com.uber.marmaray.utilities.SchemaUtil;
import com.uber.marmaray.utilities.StringTypes;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.hadoop.ConfigHelper;
import parquet.Preconditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@link CassandraSinkConfiguration} contains all relevant configuration properties to disperse data to Cassandra.
 * Properties include but are not limited to MapReduce job settings, optimization parameters, etc.
 *
 * We explicitly assume all whitespace characters are trimmed from values in the yaml reader so we don't have
 * to sanitize the values again here.
 */
@Slf4j
public class CassandraSinkConfiguration implements Serializable, IMetricable {

    public static final String CASSANDRA_PREFIX_ONLY = "cassandra.";
    public static final String CASS_COMMON_PREFIX = Configuration.MARMARAY_PREFIX + CASSANDRA_PREFIX_ONLY;
    public static final String HADOOP_COMMON_PREFIX = Configuration.MARMARAY_PREFIX + "hadoop.";
    // *** Cassandra Configuration Settings ***
    public static final String OUTPUT_THRIFT_PORT = CASS_COMMON_PREFIX + "output.thrift.port";

    public static final String NATIVE_TRANSPORT_PORT =
            HADOOP_COMMON_PREFIX + CASSANDRA_PREFIX_ONLY + "output.native.port";
    // Note: CassandraUnit uses port 9142 so it won't disturb any local default cassandra installation
    public static final String DEFAULT_OUTPUT_NATIVE_PORT = "9042";

    public static final String INITIAL_HOSTS = HADOOP_COMMON_PREFIX + CASSANDRA_PREFIX_ONLY + "output.thrift.address";

    // this setting is currently not used yet but will eventually be needed for throttling
    public static final String STREAM_THROTTLE_MBITS = "mapreduce.output.bulkoutputformat.streamthrottlembits";

    public static final String STORAGE_PORT = CASS_COMMON_PREFIX + "storage.port";
    public static final String SSL_STORAGE_PORT = CASS_COMMON_PREFIX + "ssl.storage.port";

    public static final String DISABLE_QUERY_UNS = CASS_COMMON_PREFIX + "disable_uns";
    public static final String DISABLE_QUERY_LANGLEY = CASS_COMMON_PREFIX + "disable_langley";

    // *** End of Cassandra Configuration Settings ***1
    public static final String DEFAULT_OUTPUT_RPC_PORT = "9160";
    // *** End of Cassandra Configuration Settings ***

    // *** Dispersal Job Settings ***
    public static final String KEYSPACE = CASS_COMMON_PREFIX + "keyspace";
    public static final String TABLE_NAME = CASS_COMMON_PREFIX + "tablename";
    public static final String CLUSTER_NAME = CASS_COMMON_PREFIX + "cluster_name";
    public static final String COLUMN_LIST = CASS_COMMON_PREFIX + "column_list";
    public static final String PARTITION_KEYS = CASS_COMMON_PREFIX + "partition_keys";
    public static final String CLUSTERING_KEYS = CASS_COMMON_PREFIX + "clustering_keys";
    public static final String PARTITION_TYPE = CASS_COMMON_PREFIX + "partition_type";
    public static final String WRITTEN_TIME = CASS_COMMON_PREFIX + "written_time";
    public static final String USERNAME = CASS_COMMON_PREFIX + "username";
    public static final String PASSWORD = CASS_COMMON_PREFIX + "password";
    public static final String USE_CLIENT_SINK = CASS_COMMON_PREFIX + "use_client_sink";
    public static final String SHOULD_SKIP_INVALID_ROWS = CASS_COMMON_PREFIX + "should_skip_invalid_rows";

    // optional field for customers to timestamp their data
    public static final String TIMESTAMP = CASS_COMMON_PREFIX + SchemaUtil.DISPERSAL_TIMESTAMP;
    public static final String TIMESTAMP_IS_LONG_TYPE = CASS_COMMON_PREFIX + "timestamp_is_long_type";
    public static final String TIMESTAMP_FIELD_NAME = CASS_COMMON_PREFIX + "timestamp_field_name";
    public static final String DEFAULT_DISPERSAL_TIMESTAMP_FIELD_NAME = "timestamp";
    public static final boolean DEFAULT_TIMESTAMP_IS_LONG_TYPE = false;

    // File path containing datacenter info on each machine
    public static final String DC_PATH = "data_center_path";

    public static final String DATACENTER = CASS_COMMON_PREFIX + "datacenter";

    public static final String TIME_TO_LIVE = CASS_COMMON_PREFIX + "time_to_live";

    // feature flags
    public static final String ENABLE_IGNORE_HOSTS = CASS_COMMON_PREFIX + "enable_ignore_hosts";
    public static final boolean DEFAULT_ENABLE_IGNORE_HOSTS = false;

    public static final long DEFAULT_TIME_TO_LIVE = 0L;

    public static final String MAX_BATCH_SIZE_MB = CASS_COMMON_PREFIX + "max_batch_size_mb";
    public static final long DEFAULT_MAX_BATCH_SIZE_MB = -1;
    public static final long DISABLE_BATCH = -1;
    public static final String MIN_BATCH_DURATION_SECONDS = CASS_COMMON_PREFIX + "min_batch_duration_seconds";
    public static final long DEFAULT_MIN_BATCH_DURATION_SECONDS = 0;

    private static final Splitter splitter = Splitter.on(StringTypes.COMMA);

    @Getter
    protected final Configuration conf;

    @Getter
    protected final List<String> initialHosts;

    /**
     * Optional field. This functionality enables a subset of columns to be dispersed from the data source and
     * not forcing the user to either disperse all or none of the data.
     * This set must include all partition and clustering keys if defined or the job will fail.
     */
    @Getter
    private final Optional<Set<String>> filteredColumns;

    @Getter
    private final List<String> partitionKeys;

    @Getter
    private final List<ClusterKey> clusteringKeys;

    @Getter
    private final PartitionType partitionType;

    @Getter
    private final Optional<String> writeTimestamp;

    @Getter
    private final String timestampFieldName;

    @Getter
    private final boolean timestampIsLongType;

    @Getter
    private final boolean enableIgnoreHosts;

    @Getter
    private final long maxBatchSizeMb;

    @Getter
    private final long minBatchDurationSeconds;

    @Getter
    private Optional<DataFeedMetrics> dataFeedMetrics = Optional.absent();

    @Getter
    private final Optional<String> writtenTime;

    public CassandraSinkConfiguration(@NonNull final Configuration conf,
                                      @NonNull final DataFeedMetrics dataFeedMetrics) {
        this(conf);
        setDataFeedMetrics(dataFeedMetrics);
    }

    public CassandraSinkConfiguration(@NonNull final Configuration conf) {
        this.conf = conf;
        ConfigUtil.checkMandatoryProperties(this.conf, this.getMandatoryProperties());

        this.partitionKeys = this.splitString(this.conf.getProperty(PARTITION_KEYS).get());

        if (this.conf.getProperty(INITIAL_HOSTS).isPresent()) {
            this.initialHosts = this.splitString(this.conf.getProperty(INITIAL_HOSTS).get());
        } else {
            this.initialHosts = new ArrayList<>();
        }

        // Source fields can be case insensitive so we convert everything to lower case for comparing
        this.filteredColumns = this.conf.getProperty(COLUMN_LIST).isPresent()
                ? Optional.of(new HashSet<>(this.splitString(this.conf.getProperty(COLUMN_LIST).get().toLowerCase())))
                : Optional.absent();

        this.clusteringKeys = this.conf.getProperty(CLUSTERING_KEYS).isPresent()
                ? initClusterKeys(this.splitString(this.conf.getProperty(CLUSTERING_KEYS).get()))
                : Collections.EMPTY_LIST;

        this.writtenTime = this.conf.getProperty(WRITTEN_TIME).isPresent()
                ? Optional.of(this.conf.getProperty(WRITTEN_TIME).get())
                : Optional.absent();

        if (this.conf.getProperty(PARTITION_TYPE).isPresent()) {
            this.partitionType = PartitionType.valueOf(this.conf.getProperty(PARTITION_TYPE)
                    .get().trim().toUpperCase());
        } else {
            this.partitionType = PartitionType.NONE;
        }

        this.writeTimestamp = this.conf.getProperty(TIMESTAMP);
        this.timestampFieldName = this.conf.getProperty(TIMESTAMP_FIELD_NAME, DEFAULT_DISPERSAL_TIMESTAMP_FIELD_NAME);
        this.timestampIsLongType = this.conf.getBooleanProperty(TIMESTAMP_IS_LONG_TYPE, DEFAULT_TIMESTAMP_IS_LONG_TYPE);
        this.enableIgnoreHosts = this.conf.getBooleanProperty(ENABLE_IGNORE_HOSTS, DEFAULT_ENABLE_IGNORE_HOSTS);
        this.maxBatchSizeMb = this.conf.getLongProperty(MAX_BATCH_SIZE_MB, DEFAULT_MAX_BATCH_SIZE_MB);
        Preconditions.checkState(this.maxBatchSizeMb > 0 || this.maxBatchSizeMb == DISABLE_BATCH,
                String.format("%s must greater than zero or %d", MAX_BATCH_SIZE_MB, DISABLE_BATCH));
        this.minBatchDurationSeconds = this.conf.getLongProperty(MIN_BATCH_DURATION_SECONDS,
                DEFAULT_MIN_BATCH_DURATION_SECONDS);
        Preconditions.checkState(this.minBatchDurationSeconds >= 0,
                String.format("%s must be non-negative", MIN_BATCH_DURATION_SECONDS));
    }

    @Override
    public void setDataFeedMetrics(@NonNull final DataFeedMetrics dataFeedMetrics) {
        this.dataFeedMetrics = Optional.of(dataFeedMetrics);
    }

    @Override
    public void setJobMetrics(@NonNull final JobMetrics jobMetrics) {
        // ignored
    }

    /**
     * Returns hadoop configuration.
     * Some example of things that could be set are the output rpc port (cassandra.output.thrift.port)
     * and other cassandra specific hadoop configuration settings.
     */
    public org.apache.hadoop.conf.Configuration getHadoopConf() {
        final org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();

        if (this.getUserName().isPresent() && this.getPassword().isPresent()) {
            ConfigHelper.setOutputKeyspaceUserNameAndPassword(
                    hadoopConf,
                    this.getUserName().get(),
                    this.getPassword().get()
            );
        }

        if (!this.conf.getProperty(DATACENTER).isPresent()) {
            // should never happen, but a sanity check
            throw new MissingPropertyException("The datacenter information is missing");
        }

        // By default use Murmur3 Partitioner for now, we may want this to be changed in future
        ConfigHelper.setOutputPartitioner(hadoopConf, Murmur3Partitioner.class.getName());

        log.info("Setting output local DC only to true");
        ConfigHelper.setOutputLocalDCOnly(hadoopConf, true);

        this.conf.getPropertiesWithPrefix(CassandraSinkConfiguration.HADOOP_COMMON_PREFIX, true).forEach(
            (key, value) -> {
                log.info("hadoop-conf-update:key:[{}]:value:[{}]", key, value);
                hadoopConf.set(key, value);
            }
        );
        return hadoopConf;
    }

    public String getKeyspace() {
        return this.getConf().getProperty(KEYSPACE).get().trim();
    }

    public String getTableName() {
        return this.getConf().getProperty(TABLE_NAME).get().trim();
    }

    public String getClusterName() {
        return this.getConf().getProperty(CLUSTER_NAME).get().trim();
    }

    public Boolean getUseClientSink() {
        return this.getConf().getBooleanProperty(USE_CLIENT_SINK, false);
    }

    public Boolean getShouldSkipInvalidRows() {
        return this.getConf().getBooleanProperty(SHOULD_SKIP_INVALID_ROWS, false);
    }

    public Optional<String> getUserName() {
        return this.getConf().getProperty(USERNAME);
    }

    public Optional<String> getPassword() {
        return this.getConf().getProperty(PASSWORD);
    }

    public Optional<String> getSSLStoragePort() {
        return this.conf.getProperty(SSL_STORAGE_PORT).isPresent()
                ? Optional.of(this.conf.getProperty(SSL_STORAGE_PORT).get())
                : Optional.absent();
    }

    public Optional<String> getStoragePort() {
        return this.conf.getProperty(STORAGE_PORT).isPresent()
                ? Optional.of(this.conf.getProperty(STORAGE_PORT).get())
                : Optional.absent();
    }

    // TTL is optional.  By default if you set it to 0 it means forever (confirm this)
    public Optional<Long> getTimeToLive() {
        return this.conf.getProperty(TIME_TO_LIVE).isPresent()
                ? Optional.of(this.conf.getLongProperty(TIME_TO_LIVE, DEFAULT_TIME_TO_LIVE))
                : Optional.absent();
    }

    // TODO - For these optional fields, consider adding a default value
    public Optional<String> getNativePort() {
        return this.conf.getProperty(NATIVE_TRANSPORT_PORT);
    }

    private List<String> getMandatoryProperties() {
        return Collections.unmodifiableList(
                Arrays.asList(
                        KEYSPACE,
                        TABLE_NAME,
                        CLUSTER_NAME,
                        PARTITION_KEYS
                ));
    }

    protected List<String> splitString(final String commaSeparatedValues) {
        return Lists.newArrayList(splitter.split(commaSeparatedValues));
    }

    private List<ClusterKey> initClusterKeys(final List<String> entries) {
        return entries.stream().map(entry -> ClusterKey.parse(entry)).collect(Collectors.toList());
    }

    public boolean isBatchEnabled() {
        return getMaxBatchSizeMb() != DISABLE_BATCH;
    }
}
