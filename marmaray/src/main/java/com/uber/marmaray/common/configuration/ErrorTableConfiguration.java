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

import com.uber.marmaray.common.sinks.hoodie.HoodieWriteStatus;
import com.uber.marmaray.utilities.ConfigUtil;
import com.uber.marmaray.utilities.ErrorTableUtil;
import lombok.Getter;
import lombok.NonNull;
import org.apache.hadoop.fs.Path;
import org.hibernate.validator.constraints.NotEmpty;
import scala.Serializable;

import java.util.Arrays;
import java.util.List;

/**
 * {@link ErrorTableConfiguration} contains the configuration used to construct ErrorTable
 */
public class ErrorTableConfiguration implements Serializable {

    public static final String ERROR_TABLE_PREFIX = Configuration.MARMARAY_PREFIX + "error_table.";
    /**
     * Flag to control whether error table is enabled
     */
    public static final String IS_ENABLED = ERROR_TABLE_PREFIX + "enabled";
    public static final boolean DEFAULT_IS_ENABLED = false;
    /**
     * Parallelism for writing error table parquet files. Note: Larger value can result into large number of small
     * files and HDFS Namenode performance may get affected.
     */
    public static final String WRITE_PARALLELISM = ERROR_TABLE_PREFIX + "parallelism";
    public static final int DEFAULT_WRITE_PARALLELISM = 1;
    /**
     * Destination folder where error table files will be written
     */
    public static final String DESTINATION_PATH = ERROR_TABLE_PREFIX + "dest_path";
    /**
     * Flag to control whether error table is written to date partition
     */
    public static final String IS_DATE_PARTITIONED = ERROR_TABLE_PREFIX + "date_partitioned";
    public static final boolean DEFAULT_IS_DATE_PARTITIONED = true;

    @Getter
    private final Configuration conf;
    @Getter
    private final boolean isEnabled;
    @Getter
    private Path destPath;
    @Getter
    private final int writeParallelism;
    @Getter
    private final boolean isDatePartitioned;

    public ErrorTableConfiguration(@NonNull final Configuration conf) {
        this.conf = conf;
        this.isEnabled = conf.getBooleanProperty(IS_ENABLED, DEFAULT_IS_ENABLED);
        if (this.isEnabled()) {
            ConfigUtil.checkMandatoryProperties(this.conf, getMandatoryProperties());
            this.destPath = new Path(conf.getProperty(DESTINATION_PATH).get());
        }
        this.isDatePartitioned = conf.getBooleanProperty(IS_DATE_PARTITIONED, DEFAULT_IS_DATE_PARTITIONED);
        this.writeParallelism = conf.getIntProperty(WRITE_PARALLELISM, DEFAULT_WRITE_PARALLELISM);
    }

    /**
     * @return hoodie configuration.
     */
    public HoodieConfiguration getHoodieConfiguration(@NonNull final Configuration conf,
                                                      @NotEmpty final String schema,
                                                      @NotEmpty final String tableKey,
                                                      @NotEmpty final String errorTableKey,
                                                      final boolean errorMetricsEnabled) {
        final HoodieConfiguration hoodieConf = new HoodieConfiguration(conf, tableKey);
        final String errorTableName = getErrorTableName(hoodieConf.getTableName());

        final HoodieConfiguration.Builder builder = HoodieConfiguration.newBuilder(conf, errorTableKey)
                .withSchema(schema)
                .withTableName(errorTableName)
                .withBasePath(this.getDestPath().toString())
                .withBulkInsertParallelism(this.getWriteParallelism())
                .enableMetrics(errorMetricsEnabled)
                .withWriteStatusClass(HoodieWriteStatus.class);
        // TODO T1793431 fix error metrics and enable metrics
        if (errorMetricsEnabled) {
            final String errorMetricsPrefix = getErrorMetricsPrefix(hoodieConf.getHoodieMetricsPrefix());
            builder.withMetricsPrefix(errorMetricsPrefix);
        }
        return builder.build();
    }

    /**
     * @return hoodie error table name.
     */
    private String getErrorTableName(@NotEmpty final String hoodieTableName) {
        return hoodieTableName + ErrorTableUtil.ERROR_TABLE_SUFFIX;
    }

    /**
     * @return hoodie error metrics prefix.
     */
    private String getErrorMetricsPrefix(@NotEmpty final String metricsPrefix) {
        return metricsPrefix + ErrorTableUtil.ERROR_TABLE_SUFFIX;
    }

    public static List<String> getMandatoryProperties() {
        return Arrays.asList(DESTINATION_PATH);
    }
}
