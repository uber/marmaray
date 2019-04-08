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
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.model.HoodieCleaningPolicy;
import com.uber.hoodie.common.table.HoodieTableConfig;
import com.uber.hoodie.config.HoodieCompactionConfig;
import com.uber.hoodie.config.HoodieIndexConfig;
import com.uber.hoodie.config.HoodieMetricsConfig;
import com.uber.hoodie.config.HoodieStorageConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.exceptions.MissingPropertyException;
import com.uber.marmaray.utilities.ConfigUtil;
import com.uber.marmaray.utilities.StringTypes;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.hibernate.validator.constraints.NotEmpty;
import scala.Serializable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * {@link HoodieConfiguration} class holds hoodie configurations.
 *
 * All common properties start with {@link #HOODIE_COMMON_PROPERTY_PREFIX}.
 * All table properties start with {@link #HOODIE_TABLES_PREFIX}.
 */
@Slf4j
public class HoodieConfiguration implements Serializable {

    public static final String HOODIE_COMMON_PROPERTY_PREFIX = Configuration.MARMARAY_PREFIX + "hoodie.%s.";
    public static final String HOODIE_TABLES_PREFIX = "tables.%s";
    public static final String HOODIE_DEFAULT_PREFIX = "default";
    /**
     * Hoodie table name
     */
    public static final String HOODIE_TABLE_NAME = HOODIE_COMMON_PROPERTY_PREFIX + "table_name";
    /**
     * Hoodie base path
     */
    public static final String HOODIE_BASE_PATH = HOODIE_COMMON_PROPERTY_PREFIX + "base_path";
    /**
     * Schema for Hoodie dataset
     */
    public static final String HOODIE_AVRO_SCHEMA = HOODIE_COMMON_PROPERTY_PREFIX + "schema";
    /**
     * Flag to control whether it should combine before insert
     */
    public static final String HOODIE_COMBINE_BEFORE_INSERT = HOODIE_COMMON_PROPERTY_PREFIX + "combine_before_insert";
    public static final boolean DEFAULT_HOODIE_COMBINE_BEFORE_INSERT = false;
    /**
     * Flag to control whether it should combine before upsert
     */
    public static final String HOODIE_COMBINE_BEFORE_UPSERT = HOODIE_COMMON_PROPERTY_PREFIX + "combine_before_upsert";
    public static final boolean DEFAULT_HOODIE_COMBINE_BEFORE_UPSERT = false;
    /**
     * Hoodie bulk_insert, insert & upsert parallelism
     * The default value is the same as HoodieWriteConfig's default (a private variable)
     */
    public static final String HOODIE_BULKINSERT_PARALLELISM
            = HOODIE_COMMON_PROPERTY_PREFIX + "bulkinsert_parallelism";
    public static final String HOODIE_INSERT_PARALLELISM = HOODIE_COMMON_PROPERTY_PREFIX + "insert_parallelism";
    public static final String HOODIE_UPSERT_PARALLELISM = HOODIE_COMMON_PROPERTY_PREFIX + "upsert_parallelism";
    public static final int DEFAULT_HOODIE_PARALLELISM = 200;
    /**
     * Auto tune insert parallelism for bulk insert
     */
    public static final String HOODIE_AUTO_TUNE_PARALLELISM =
            HOODIE_COMMON_PROPERTY_PREFIX + "auto_tune_parallelism";
    public static final boolean DEFAULT_AUTO_TUNE_PARALLELISM = true;
    /**
     * Target file size if auto tuning is enabled for insert parallelism.
     */
    public static final String HOODIE_TARGET_FILE_SIZE =
            HOODIE_COMMON_PROPERTY_PREFIX + "auto_target_file_size";
    // default is set to 1GB which is between HOODIE_COMPACTION_SMALL_FILE_SIZE_LIMIT and
    // HOODIE_PARQUET_MAX_FILE_SIZE.
    public static final long DEFAULT_HOODIE_TARGET_FILE_SIZE = FileUtils.ONE_GB;
    /**
      * Write buffer limit in bytes to be used for bulk insert
      */
    public static final String HOODIE_INSERT_BUFFER_MEMORY_BYTES =
        HOODIE_COMMON_PROPERTY_PREFIX + "insert_buffer_memory_bytes";
    public static final int DEFAULT_HOODIE_INSERT_BUFFER_MEMORY_BYTES = (int) (32 * FileUtils.ONE_MB);

    // Hoodie Compaction parameters
    /**
     * Hoodie enable auto clean
     */
    public static final String HOODIE_ENABLE_AUTO_CLEAN = HOODIE_COMMON_PROPERTY_PREFIX + "enable_auto_clean";
    public static final boolean DEFAULT_HOODIE_ENABLE_AUTO_CLEAN = true;
    /**
     * Hoodie cleaner policy
     */
    public static final String HOODIE_CLEANER_POLICY = HOODIE_COMMON_PROPERTY_PREFIX + "cleaner_policy";
    public static final String DEFAULT_HOODIE_CLEANER_POLICY = "KEEP_LATEST_COMMITS";
    /**
     * Hoodie cleaner commits retained
     */
    public static final String HOODIE_CLEANER_COMMITS_RETAINED =
            HOODIE_COMMON_PROPERTY_PREFIX + "cleaner_commits_retained";
    public static final int DEFAULT_HOODIE_CLEANER_COMMITS_RETAINED = 10;
    /**
     * Hoodie cleaner versions retained
     */
    public static final String HOODIE_CLEANER_VERSIONS_RETAINED =
            HOODIE_COMMON_PROPERTY_PREFIX + "cleaner_versions_retained";
    public static final int DEFAULT_HOODIE_CLEANER_VERSIONS_RETAINED = 3;
    /**
     * Hoodie Data partitioner
     */
    public static final String HOODIE_DATA_PARTITIONER = HOODIE_COMMON_PROPERTY_PREFIX + "data_partitioner";
    /**
     * Hoodie compaction small file size
     */
    public static final String HOODIE_COMPACTION_SMALL_FILE_SIZE_LIMIT =
            HOODIE_COMMON_PROPERTY_PREFIX + "compaction_small_file_size_limit";
    public static final long DEFAULT_HOODIE_COMPACTION_SMALL_FILE_SIZE_LIMIT = 80 * FileUtils.ONE_MB;
    /**
     * Hoodie Storage file size.
     */
    /**
     * Range for maximum parquet file size (uncompressed) is between {@link #HOODIE_COMPACTION_SMALL_FILE_SIZE_LIMIT}
     * and 6GB. Default is set to 1GB. Set this value to at least > 2.5 times
     * TODO: Reduced to int to handle current hoodie version. Should be converted back to long on hoodie upgrade
     * {@link #HOODIE_COMPACTION_SMALL_FILE_SIZE_LIMIT}.
     */
    public static final String HOODIE_PARQUET_MAX_FILE_SIZE =
            HOODIE_COMMON_PROPERTY_PREFIX + "parquet_max_file_size";
    public static final int DEFAULT_HOODIE_PARQUET_MAX_FILE_SIZE = (int) FileUtils.ONE_GB;
    /**
     * Hoodie insert split size
     */
    public static final String HOODIE_INSERT_SPLIT_SIZE = HOODIE_COMMON_PROPERTY_PREFIX + "insert_split_size";

    // Hoodie bloom index properties
    /**
     * Hoodie bloom index filter FPP
     */
    public static final String HOODIE_BLOOM_FILTER_FPP = HOODIE_COMMON_PROPERTY_PREFIX + "bloom_filter_fpp";
    public static final double DEFAULT_HOODIE_BLOOM_FILTER_FPP =
            Double.parseDouble(HoodieIndexConfig.DEFAULT_BLOOM_FILTER_FPP);
    /**
     * Hoodie bloom filter num entries
     */
    public static final String HOODIE_BLOOM_FILTER_NUM_ENTRIES =
            HOODIE_COMMON_PROPERTY_PREFIX + "bloom_filter_num_entries";
    public static final int DEFAULT_HOODIE_BLOOM_FILTER_NUM_ENTRIES =
            Integer.parseInt(HoodieIndexConfig.DEFAULT_BLOOM_FILTER_NUM_ENTRIES);
    /**
     * Hoodie bloom index parallelism
     */
    public static final String HOODIE_BLOOM_INDEX_PARALLELISM =
            HOODIE_COMMON_PROPERTY_PREFIX + "bloom_index_parallelism";
    public static final int DEFAULT_HOODIE_BLOOM_INDEX_PARALLELISM = 1024;

    // Hoodie Write Status config.
    /**
     * Hoodie Write status class
     */
    public static final String HOODIE_WRITE_STATUS_CLASS = HOODIE_COMMON_PROPERTY_PREFIX + "write_status_class";
    public static final String DEFAULT_HOODIE_WRITE_STATUS_CLASS = WriteStatus.class.getCanonicalName();
    // Hoodie metrics config.
    /**
     * Hoodie metrics prefix
     */
    public static final String HOODIE_METRICS_PREFIX = HOODIE_COMMON_PROPERTY_PREFIX + "metrics_prefix";
    /**
     * Hoodie enable metrics
     */
    public static final String HOODIE_ENABLE_METRICS = HOODIE_COMMON_PROPERTY_PREFIX + "enable_metrics";
    public static final boolean DEFAULT_HOODIE_ENABLE_METRICS = true;

    /**
     * Rollback inflight commits.
     */
    public static final String HOODIE_ROLLBACK_INFLIGHT_COMMITS =
            HOODIE_COMMON_PROPERTY_PREFIX + "rollback_inflight_commits";
    public static final boolean DEFAULT_HOODIE_ROLLBACK_INFLIGHT_COMMITS = true;

    /**
     * Payload class to use.
     */
    public static final String HOODIE_PAYLOAD_CLASS_NAME =
            HOODIE_COMMON_PROPERTY_PREFIX + "paylaod_class_name";
    // default isn't used to set a vaule, but instead to detect if it was set
    public static final String DEFAULT_HOODIE_PAYLOAD_CLASS_NAME = "default";

    @Getter
    private final Configuration conf;
    @Getter
    private final String tableKey;
    @Getter
    private final Optional<String> version;

    public HoodieConfiguration(@NonNull final Configuration conf, @NotEmpty final String tableKey,
            @NonNull final Optional<String> version) {
        this.conf = conf;
        this.tableKey = tableKey;
        this.version = version;
        ConfigUtil.checkMandatoryProperties(this.conf, getMandatoryProperties());
    }

    public HoodieConfiguration(@NonNull final Configuration conf, @NotEmpty final String tableKey) {
        this(conf, tableKey, Optional.absent());
    }

    /**
     * @return List of mandatory properties.
     */
    public List<String> getMandatoryProperties() {
        return Collections.unmodifiableList(Arrays.asList(getTablePropertyKey(HOODIE_TABLE_NAME, this.tableKey),
                getTablePropertyKey(HOODIE_BASE_PATH, this.tableKey)));
    }

    /**
     * @return hoodie base path directory
     */
    public String getBasePath() {
        // HOODIE_BASE_PATH is a mandatory property. Please check {#getMandatoryProperties()}.
        return this.getConf().getProperty(getTablePropertyKey(HOODIE_BASE_PATH, this.tableKey)).get();
    }

    /**
     * @return hoodie table name.
     */
    public String getTableName() {
        return this.getConf().getProperty(getTablePropertyKey(HOODIE_TABLE_NAME, this.tableKey)).get();
    }

    /**
     * @return hoodie metrics prefix.
     * */
    public String getHoodieMetricsPrefix() {
        return this.getConf().getProperty(getTablePropertyKey(HOODIE_METRICS_PREFIX, this.tableKey)).get();
    }

    public String getHoodieDataPartitioner(@NotEmpty final String defaultDataPartitioner) {
        return this.getConf().getProperty(getTablePropertyKey(HOODIE_DATA_PARTITIONER, this.tableKey),
            defaultDataPartitioner);
    }

    /**
     * @return true if {@link com.uber.hoodie.HoodieWriteClient} should rollback inflight commits from previous write
     * call.
     */
    public boolean shouldRollbackInFlight() {
        return getProperty(HOODIE_ROLLBACK_INFLIGHT_COMMITS, DEFAULT_HOODIE_ROLLBACK_INFLIGHT_COMMITS);
    }

    /**
     * @return true if auto-clean is enabled.
     */
    public boolean shouldAutoClean() {
        return getProperty(HOODIE_ENABLE_AUTO_CLEAN, DEFAULT_HOODIE_ENABLE_AUTO_CLEAN);
    }

    /**
     * @return true if insert parallelism needs to be auto tuned.
     */
    public boolean shouldAutoTuneParallelism() {
        return getProperty(HOODIE_AUTO_TUNE_PARALLELISM, DEFAULT_AUTO_TUNE_PARALLELISM);
    }

    /**
     * @return expected target file size. Needs to be set if {@link #HOODIE_AUTO_TUNE_PARALLELISM} is enabled.
     */
    public long getTargetFileSize() {
        return getProperty(HOODIE_TARGET_FILE_SIZE, DEFAULT_HOODIE_TARGET_FILE_SIZE);
    }

    /**
     * Used for updating table property
     */
    public void setTableProperty(@NotEmpty final String tablePropertyKey, @NotEmpty final String value) {
        this.conf.setProperty(getTablePropertyKey(tablePropertyKey, this.tableKey), value);
    }

    /**
     * @return returns hoodie properties
     */
    public Properties getHoodieInitProperties() {
        final Properties props = new Properties();
        props.put(HoodieTableConfig.HOODIE_TABLE_NAME_PROP_NAME, this.getTableName());
        return props;
    }

    /**
     * @return hoodie bulk insert parallelism
     */
    public int getBulkInsertParallelism() {
        return getProperty(HOODIE_BULKINSERT_PARALLELISM, DEFAULT_HOODIE_PARALLELISM);
    }

    /**
     * @return hoodie insert parallelism
     */
    public int getInsertParallelism() {
        return getProperty(HOODIE_INSERT_PARALLELISM, DEFAULT_HOODIE_PARALLELISM);
    }

    /**
     * @return hoodie upsert parallelism
     */
    public int getUpsertParallelism() {
        return getProperty(HOODIE_UPSERT_PARALLELISM, DEFAULT_HOODIE_PARALLELISM);
    }

    /**
     * @return {@link HoodieWriteConfig}. It uses {@link #conf} to create {@link HoodieWriteConfig}. If any property is
     * missing then it will throw {@link MissingPropertyException}.
     */
    public HoodieWriteConfig getHoodieWriteConfig() {
        final HoodieWriteConfig.Builder builder = HoodieWriteConfig.newBuilder();
        try {
            // This table name is used for sending metrics to graphite by hoodie. It expects table name to be without
            // ".".
            builder.forTable(getTableName().replaceAll("\\.", StringTypes.UNDERSCORE));
            builder.withPath(getBasePath());
            final boolean combineBeforeInsert =
                    getProperty(HOODIE_COMBINE_BEFORE_INSERT, DEFAULT_HOODIE_COMBINE_BEFORE_INSERT);
            final boolean combineBeforeUpsert =
                    getProperty(HOODIE_COMBINE_BEFORE_UPSERT, DEFAULT_HOODIE_COMBINE_BEFORE_UPSERT);
            builder.combineInput(combineBeforeInsert, combineBeforeUpsert);
            final String schemaPropertyKey = getTablePropertyKey(HOODIE_AVRO_SCHEMA, this.tableKey);
            final Optional<String> schema = this.conf.getProperty(schemaPropertyKey);
            if (!schema.isPresent()) {
                throw new MissingPropertyException(schemaPropertyKey);
            }
            builder.withSchema(schema.get());
            builder.withParallelism(this.getInsertParallelism(), this.getUpsertParallelism())
                    .withBulkInsertParallelism(this.getBulkInsertParallelism());
            builder.withAutoCommit(false);

            // Date partitioning.
            builder.withAssumeDatePartitioning(true);

            // Hoodie compaction config.
            final HoodieCompactionConfig.Builder compactionConfigBuilder = HoodieCompactionConfig.newBuilder();

            compactionConfigBuilder.withCleanerPolicy(HoodieCleaningPolicy
                    .valueOf(getProperty(HOODIE_CLEANER_POLICY, DEFAULT_HOODIE_CLEANER_POLICY)));

            // set the payload class if it has been set in configuration
            final String payloadClass = getProperty(HOODIE_PAYLOAD_CLASS_NAME, DEFAULT_HOODIE_PAYLOAD_CLASS_NAME);
            if (!DEFAULT_HOODIE_PAYLOAD_CLASS_NAME.equals(payloadClass)) {
                compactionConfigBuilder.withPayloadClass(payloadClass);
            }
            compactionConfigBuilder.retainCommits(
                    getProperty(HOODIE_CLEANER_COMMITS_RETAINED, DEFAULT_HOODIE_CLEANER_COMMITS_RETAINED));
            compactionConfigBuilder.retainFileVersions(
                    getProperty(HOODIE_CLEANER_VERSIONS_RETAINED, DEFAULT_HOODIE_CLEANER_VERSIONS_RETAINED));
            final Integer insertSplitSize = getProperty(HOODIE_INSERT_SPLIT_SIZE, -1);
            if (insertSplitSize > 0) {
                compactionConfigBuilder.autoTuneInsertSplits(false);
                compactionConfigBuilder.insertSplitSize(insertSplitSize);
            } else {
                compactionConfigBuilder.autoTuneInsertSplits(true);
            }
            compactionConfigBuilder.compactionSmallFileSize(
                    getProperty(HOODIE_COMPACTION_SMALL_FILE_SIZE_LIMIT,
                            DEFAULT_HOODIE_COMPACTION_SMALL_FILE_SIZE_LIMIT));
            compactionConfigBuilder.withAutoClean(shouldAutoClean());
            builder.withCompactionConfig(compactionConfigBuilder.build());

            // Hoodie storage config.
            builder.withStorageConfig(
                    HoodieStorageConfig.newBuilder().limitFileSize(
                            getProperty(HOODIE_PARQUET_MAX_FILE_SIZE, DEFAULT_HOODIE_PARQUET_MAX_FILE_SIZE)
                    ).build());

            // Hoodie index config
            builder.withIndexConfig(new HoodieIndexConfiguration(getConf(), getTableKey()).configureHoodieIndex());

            // Hoodie metrics config
            final boolean enableMetrics = getProperty(HOODIE_ENABLE_METRICS, DEFAULT_HOODIE_ENABLE_METRICS);
            if (enableMetrics) {
                final HoodieMetricsConfig.Builder hoodieMetricsConfigBuilder = HoodieMetricsConfig.newBuilder();
                final String hoodieMetricsPropertyKey = getTablePropertyKey(HOODIE_METRICS_PREFIX, this.tableKey);
                final Optional<String> hoodieMetricsProperty = this.conf.getProperty(hoodieMetricsPropertyKey);
                if (!hoodieMetricsProperty.isPresent()) {
                    throw new MissingPropertyException(hoodieMetricsPropertyKey);
                }
                hoodieMetricsConfigBuilder.usePrefix(hoodieMetricsProperty.get());
                hoodieMetricsConfigBuilder.on(getProperty(HOODIE_ENABLE_METRICS, DEFAULT_HOODIE_ENABLE_METRICS));
                builder.withMetricsConfig(hoodieMetricsConfigBuilder.build());
            }
            // Write status StorageLevel.
            builder.withWriteStatusStorageLevel("DISK_ONLY");
            final String writeStatusClassName =
                    getProperty(HOODIE_WRITE_STATUS_CLASS, DEFAULT_HOODIE_WRITE_STATUS_CLASS);
            try {
                builder.withWriteStatusClass(
                        (Class<? extends WriteStatus>) Class.forName(writeStatusClassName));
            } catch (ClassNotFoundException e) {
                final String errorStr =
                        String.format("error loading hoodie write status class :{}", writeStatusClassName);
                log.error(errorStr);
                throw new JobRuntimeException(errorStr, e);
            }

            // enable tmp directory writes for hoodie.
            builder.withUseTempFolderCopyOnWriteForCreate(true);

            // enabled the renaming for copy detection on merge
            builder.withUseTempFolderCopyOnWriteForMerge(true);

            return builder.build();
        } catch (IllegalArgumentException e) {
            throw new MissingPropertyException(e.getMessage(), e);
        }
    }

    /**
     * It will read property value from table and default namespace. Value will be returned in following order.
     * For example for propertyKey ("common.hoodie.%s.insert_split_size")
     * 1) table specific value ("common.hoodie.tables.table1.insert_split_size" defined in {@link Configuration})
     * 2) default hoodie property value ("common.hoodie.default.insert_split_size" defined in {@link Configuration})
     * 3) default value specified. (passed in as an argument).
     *
     * @param propertyKey  hoodie property key
     * @param defaultValue default value of the property
     * @param <T>          DataType of the property
     */
    public <T> T getProperty(@NotEmpty final String propertyKey,
            @NonNull final T defaultValue) {
        final String defaultKey = getDefaultPropertyKey(propertyKey);
        final String tableKey = getTablePropertyKey(propertyKey, this.tableKey);
        final T retValue = Configuration.getProperty(this.conf, defaultKey, defaultValue);
        return Configuration.getProperty(this.conf, tableKey, retValue);
    }

    public static String getTablePropertyKey(@NotEmpty final String propertyKey, @NotEmpty final String tableKey) {
        return String.format(propertyKey, String.format(HOODIE_TABLES_PREFIX, tableKey));
    }

    public static String getDefaultPropertyKey(@NotEmpty final String propertyKey) {
        return String.format(propertyKey, HOODIE_DEFAULT_PREFIX);
    }

    public static Builder newBuilder(@NotEmpty final String tableKey) {
        return newBuilder(new Configuration(), tableKey);
    }

    public static Builder newBuilder(@NonNull final Configuration conf, @NotEmpty final String tableKey) {
        return new Builder(conf, tableKey);
    }

    /**
     * Builder class to build {@link HoodieConfiguration}.
     */
    public static final class Builder {

        private final Configuration conf;
        private final String tableKey;
        private Optional<String> version = Optional.absent();

        private Builder(@NonNull final Configuration conf, @NotEmpty final String tableKey) {
            this.conf = conf;
            this.tableKey = tableKey;
        }

        public Builder withTableName(@NotEmpty final String tableName) {
            this.conf.setProperty(getTablePropertyKey(HOODIE_TABLE_NAME, this.tableKey), tableName);
            return this;
        }

        public Builder withBasePath(@NotEmpty final String basePath) {
            this.conf.setProperty(getTablePropertyKey(HOODIE_BASE_PATH, this.tableKey), basePath);
            return this;
        }

        public Builder withSchema(@NotEmpty final String schema) {
            this.conf.setProperty(getTablePropertyKey(HOODIE_AVRO_SCHEMA, this.tableKey), schema);
            return this;
        }

        public Builder withBulkInsertParallelism(final int parallelism) {
            this.conf.setProperty(
                    getTablePropertyKey(HOODIE_BULKINSERT_PARALLELISM, this.tableKey), Integer.toString(parallelism));
            return this;
        }

        public Builder withInsertParallelism(final int parallelism) {
            this.conf.setProperty(
                    getTablePropertyKey(HOODIE_INSERT_PARALLELISM, this.tableKey), Integer.toString(parallelism));
            return this;
        }

        public Builder withUpsertParallelism(final int parallelism) {
            this.conf.setProperty(
                    getTablePropertyKey(HOODIE_UPSERT_PARALLELISM, this.tableKey), Integer.toString(parallelism));
            return this;
        }

        public Builder withMetricsPrefix(@NotEmpty final String metricsPrefix) {
            this.conf.setProperty(getTablePropertyKey(HOODIE_METRICS_PREFIX, tableKey), metricsPrefix);
            return this;
        }

        public Builder withCombineBeforeInsert(final boolean combineBeforeInsert) {
            this.conf.setProperty(getTablePropertyKey(HOODIE_COMBINE_BEFORE_INSERT, tableKey),
                Boolean.toString(combineBeforeInsert));
            return this;
        }

        public Builder withCombineBeforeUpsert(final boolean combineBeforeUpsert) {
            this.conf.setProperty(getTablePropertyKey(HOODIE_COMBINE_BEFORE_UPSERT, tableKey),
                Boolean.toString(combineBeforeUpsert));
            return this;
        }

        public Builder enableMetrics(final boolean enableMetrics) {
            this.conf.setProperty(
                    getTablePropertyKey(HOODIE_ENABLE_METRICS, tableKey), Boolean.toString(enableMetrics));
            return this;
        }

        public Builder autoTuneParallelism(final boolean autoTuneParallelism) {
            this.conf.setProperty(
                    getTablePropertyKey(HOODIE_AUTO_TUNE_PARALLELISM, tableKey), Boolean.toString(autoTuneParallelism));
            return this;
        }

        public Builder withWriteStatusClass(@NotEmpty final Class writeStatusClass) {
            this.conf.setProperty(
                    getTablePropertyKey(HOODIE_WRITE_STATUS_CLASS, tableKey), writeStatusClass.getCanonicalName());
            return this;
        }

        public Builder withVersion(@NotEmpty final String version) {
            this.version = Optional.of(version);
            return this;
        }

        public HoodieConfiguration build() {
            return new HoodieConfiguration(this.conf, this.tableKey, this.version);
        }
    }
}
