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

import com.google.common.base.Preconditions;
import com.uber.hoodie.config.HoodieIndexConfig;
import com.uber.hoodie.index.HoodieIndex;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.utilities.StringTypes;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.hibernate.validator.constraints.NotEmpty;

import java.io.IOException;

@Slf4j
public class HoodieIndexConfiguration extends HoodieConfiguration {

    // Hoodie Index config
    public static final String HOODIE_INDEX_PROPERTY_PREFIX =
            HoodieConfiguration.HOODIE_COMMON_PROPERTY_PREFIX + "index.";
    /**
     * Hoodie index types
     */
    public static final String HOODIE_BLOOM_INDEX = "bloom";
    public static final String HOODIE_HBASE_INDEX = "hbase";
    public static final String HOODIE_HBASE_INDEX_PREFIX = "hbase.";
    public static final String HOODIE_INDEX_TYPE = HOODIE_INDEX_PROPERTY_PREFIX + "type";
    public static final String HOODIE_INDEX_ZKNODE = "zknode.";
    public static final String DEFAULT_HOODIE_INDEX_TYPE = HOODIE_BLOOM_INDEX;
    /**
     * Hoodie index zookeeper
     */
    public static final String HOODIE_INDEX_ZOOKEEPER_QUORUM =
            HOODIE_INDEX_PROPERTY_PREFIX + "zookeeper_quorum";
    public static final String HOODIE_INDEX_ZOKEEPER_PORT = HOODIE_INDEX_PROPERTY_PREFIX + "zookeeper_port";
    public static final String HOODIE_INDEX_HBASE_ZK_ZNODEPARENT =
            HOODIE_INDEX_PROPERTY_PREFIX + HOODIE_HBASE_INDEX_PREFIX + HOODIE_INDEX_ZKNODE + "path";
    /**
     * Hoodie index get batch size
     */
    public static final String HOODIE_INDEX_GET_BATCH_SIZE =
            HOODIE_INDEX_PROPERTY_PREFIX + "get_batch_size";
    public static final int DEFAULT_HOODIE_INDEX_GET_BATCH_SIZE = 1000;
    /**
     * Hoodie index QPS fraction
     */
    public static final String HOODIE_INDEX_QPS_FRACTION = HOODIE_INDEX_PROPERTY_PREFIX + "qps_fraction";
    public static final double DEFAULT_HOODIE_INDEX_QPS_FRACTION = 0.125f;
    /**
     * Hoodie index max QPS per region server
     */
    public static final String HOODIE_INDEX_MAX_QPS_PER_REGION_SERVER =
            HOODIE_INDEX_PROPERTY_PREFIX + "max_qps_per_region_server";
    public static final int DEFAULT_HOODIE_INDEX_MAX_QPS_PER_REGION_SERVER = 400;
    public static final String DEFAULT_VERSION = "";

    /**
     * Hoodie HBase index table name. Required if the index type is hbase.
     */
    public static final String HOODIE_HBASE_INDEX_TABLE_NAME =
            HOODIE_INDEX_PROPERTY_PREFIX + "hbase_index_table";

    @Getter
    private final Configuration conf;
    @Getter
    private final String tableKey;

    public HoodieIndexConfiguration(@NonNull final Configuration conf, @NotEmpty final String tableKey) {
        super(conf, tableKey);
        this.conf = conf;
        this.tableKey = tableKey;
    }

    public HoodieIndex.IndexType getHoodieIndexType() {
        final String indexName = getProperty(HOODIE_INDEX_TYPE, DEFAULT_HOODIE_INDEX_TYPE);
        if (HOODIE_BLOOM_INDEX.equals(indexName.toLowerCase())) {
            return HoodieIndex.IndexType.BLOOM;
        } else if (HOODIE_HBASE_INDEX.equals(indexName.toLowerCase())) {
            return HoodieIndex.IndexType.HBASE;
        } else {
            throw new IllegalStateException("Unsupported index type " + indexName);
        }
    }

    public String getHoodieIndexZookeeperQuorum() {
        final String value = getProperty(HOODIE_INDEX_ZOOKEEPER_QUORUM, StringTypes.EMPTY);
        Preconditions.checkState(!value.isEmpty(), "%s must not be empty", HOODIE_INDEX_ZOOKEEPER_QUORUM);
        return value;
    }

    public String getHoodieHbaseIndexTableName() {
        final String value = getProperty(HOODIE_HBASE_INDEX_TABLE_NAME, StringTypes.EMPTY);
        Preconditions.checkState(!value.isEmpty(), "%s must not be empty", HOODIE_HBASE_INDEX_TABLE_NAME);
        return value;
    }

    public int getHoodieIndexZookeeperPort() {
        final int value = getProperty(HOODIE_INDEX_ZOKEEPER_PORT, 0);
        Preconditions.checkState(value > 0, "%s must be greater than zero", HOODIE_INDEX_ZOKEEPER_PORT);
        return value;
    }

    public String getZkZnodeParent() {
        final String value = getProperty(HOODIE_INDEX_HBASE_ZK_ZNODEPARENT, StringTypes.EMPTY);
        Preconditions.checkState(!value.isEmpty(), "%s must always be set", HOODIE_INDEX_HBASE_ZK_ZNODEPARENT);
        return value;
    }

    public int getHoodieIndexMaxQpsPerRegionServer() {
        final int value = getProperty(HOODIE_INDEX_MAX_QPS_PER_REGION_SERVER,
                DEFAULT_HOODIE_INDEX_MAX_QPS_PER_REGION_SERVER);
        Preconditions.checkState(value > 0, "%s must be greater than zero", HOODIE_INDEX_MAX_QPS_PER_REGION_SERVER);
        return value;
    }

    public double getHoodieIndexQPSFraction() {
        final double value = getProperty(HOODIE_INDEX_QPS_FRACTION, DEFAULT_HOODIE_INDEX_QPS_FRACTION);
        Preconditions.checkState(value > 0 && value <= 1, "%s must be between 0 and 1", HOODIE_INDEX_QPS_FRACTION);
        return value;
    }

    public int getHoodieIndexGetBatchSize() {
        final int value = getProperty(HOODIE_INDEX_GET_BATCH_SIZE, DEFAULT_HOODIE_INDEX_GET_BATCH_SIZE);
        Preconditions.checkState(value > 0, "%s must be greater than zero", HOODIE_INDEX_GET_BATCH_SIZE);
        return value;
    }

    /**
     * Configure the Hoodie HBase index.
     */
    public HoodieIndexConfig configureHoodieIndex() {
        final String version;
        if (getVersion().isPresent()) {
            version = getVersion().get();
        } else {
            version = DEFAULT_VERSION;
        }
        final String topicName = getTableName();
        final HoodieIndexConfig.Builder builder = HoodieIndexConfig.newBuilder()
                .withIndexType(getHoodieIndexType());

        if (HoodieIndex.IndexType.HBASE.equals(getHoodieIndexType())) {
            final String quorum = getHoodieIndexZookeeperQuorum();
            final Integer port = getHoodieIndexZookeeperPort();
            final String zkZnodeParent  = getZkZnodeParent();
            createHbaseIndexTableIfNotExists(topicName, quorum, port.toString(), zkZnodeParent,
                    version);
        }

        return  builder.build();
    }

    public void createHbaseIndexTableIfNotExists(@NotEmpty final String dataFeed, @NotEmpty final String zkQuorum,
            @NotEmpty final String zkPort, @NotEmpty final String zkZnodeParent, @NotEmpty final String version) {
        final String tableName = getHoodieHbaseIndexTableName();
        final String family = "_s";
        final org.apache.hadoop.conf.Configuration hbaseConfig = new org.apache.hadoop.conf.Configuration();
        hbaseConfig.set("hbase.zookeeper.quorum", zkQuorum);
        hbaseConfig.set("hbase.zookeeper.property.clientPort", zkPort);
        hbaseConfig.set("zookeeper.znode.parent", zkZnodeParent);
        try {
            try (final Connection connection = ConnectionFactory.createConnection(hbaseConfig)) {
                if (!connection.getAdmin().tableExists(TableName.valueOf(tableName))) {
                    final HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
                    final HColumnDescriptor familyDesc = new HColumnDescriptor(Bytes.toBytes(family));
                    familyDesc.setBloomFilterType(BloomType.ROW);
                    familyDesc.setCompressionType(Compression.Algorithm.SNAPPY);
                    tableDesc.addFamily(familyDesc);
                    connection.getAdmin().createTable(tableDesc);
                    log.info("Created HBase table {} with family {}", tableName, family);
                } else {
                    log.debug("HBase table {} exists", tableName);
                }
            }
        } catch (IOException e) {
            //todo: better handle try catch
            log.error("Error creating HBase table {} ", tableName, e);
            throw new JobRuntimeException(e);
        }

    }
}
