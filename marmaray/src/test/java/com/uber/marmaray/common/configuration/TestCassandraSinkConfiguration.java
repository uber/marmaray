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

import com.uber.marmaray.common.PartitionType;
import com.uber.marmaray.common.exceptions.MissingPropertyException;
import com.uber.marmaray.common.schema.cassandra.ClusterKey;
import com.uber.marmaray.utilities.StringTypes;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;

public class TestCassandraSinkConfiguration {

    @Test
    public void testValidConfigurationInitialized() {
        final CassandraSinkConfiguration conf = new CassandraSinkConfiguration(getConfig(StringTypes.EMPTY));
        Assert.assertEquals(new HashSet<>(Arrays.asList("f1", "f2", "f3", "f4", "f5")),
                (conf.getFilteredColumns().get()));
        Assert.assertEquals(Arrays.asList("f1", "f2"), conf.getPartitionKeys());
        Assert.assertEquals(Arrays.asList(new ClusterKey("f3", ClusterKey.Order.ASC)), conf.getClusteringKeys());
        Assert.assertEquals(Arrays.asList("localhost1", "localhost2"), conf.getInitialHosts());
        Assert.assertEquals("testKeyspace", conf.getKeyspace());
        Assert.assertEquals("clusterFoo", conf.getClusterName());
        Assert.assertEquals("testTable", conf.getTableName());
        Assert.assertEquals(PartitionType.NORMAL, conf.getPartitionType());
    }

    @Test
    public void testSetHadoopSparkProperties() {
        final String EXECUTOR_MEM_PROP = "spark.executor.memory";
        final String DRIVER_MEM_PROP = "spark.driver.memory";
        final String NETWORK_TIMEOUT = "spark.network.timeout";
        final String FILE_FETCH_TIMEOUT = "spark.files.fetchTimeout";
        final String NUM_EXECUTORS_PROP = "spark.executor.instances";

        final Configuration conf = getConfig(StringTypes.EMPTY);
        conf.setProperty(CassandraSinkConfiguration.HADOOP_COMMON_PREFIX + EXECUTOR_MEM_PROP, "6g");
        conf.setProperty(CassandraSinkConfiguration.HADOOP_COMMON_PREFIX + DRIVER_MEM_PROP, "7g");
        conf.setProperty(CassandraSinkConfiguration.HADOOP_COMMON_PREFIX + NETWORK_TIMEOUT, "1234s");
        conf.setProperty(CassandraSinkConfiguration.HADOOP_COMMON_PREFIX + FILE_FETCH_TIMEOUT, "1111s");
        conf.setProperty(CassandraSinkConfiguration.HADOOP_COMMON_PREFIX + NUM_EXECUTORS_PROP, "32");

        final CassandraSinkConfiguration cassConf = new CassandraSinkConfiguration(conf);
        org.apache.hadoop.conf.Configuration hadoopConf = cassConf.getHadoopConf();

        Assert.assertEquals("6g", hadoopConf.get(EXECUTOR_MEM_PROP));
        Assert.assertEquals("7g", hadoopConf.get(DRIVER_MEM_PROP));
        Assert.assertEquals("1234s", hadoopConf.get(NETWORK_TIMEOUT));
        Assert.assertEquals("1111s", hadoopConf.get(FILE_FETCH_TIMEOUT));
        Assert.assertEquals("32", hadoopConf.get(NUM_EXECUTORS_PROP));
    }

    @Test(expected = MissingPropertyException.class)
    public void testMissingDataCenter() {
        final Configuration rawConf = getConfig(CassandraSinkConfiguration.DATACENTER);
        final CassandraSinkConfiguration conf = new CassandraSinkConfiguration(rawConf);
        conf.getHadoopConf();
        Assert.fail();
    }

    
    @Test(expected = MissingPropertyException.class)
    public void testMissingKeySpace() {
        final Configuration rawConf = getConfig(CassandraSinkConfiguration.KEYSPACE);
        final CassandraSinkConfiguration conf = new CassandraSinkConfiguration(rawConf);
        Assert.fail();
    }

    @Test(expected = MissingPropertyException.class)
    public void testMissingTableName() {
        final Configuration rawConf = getConfig(CassandraSinkConfiguration.TABLE_NAME);
        final CassandraSinkConfiguration conf = new CassandraSinkConfiguration(rawConf);
        Assert.fail();
    }

    @Test(expected = MissingPropertyException.class)
    public void testMissingClusterName() {
        final Configuration rawConf = getConfig(CassandraSinkConfiguration.CLUSTER_NAME);
        final CassandraSinkConfiguration conf = new CassandraSinkConfiguration(rawConf);
        Assert.fail();
    }

    @Test(expected = MissingPropertyException.class)
    public void testMissingPartitionKeys() {
        final Configuration rawConf = getConfig(CassandraSinkConfiguration.PARTITION_KEYS);
        final CassandraSinkConfiguration conf = new CassandraSinkConfiguration(rawConf);
        Assert.fail();
    }

    private Configuration getConfig(String propToExclude) {
        final Configuration conf = new Configuration();

        conf.setProperty(CassandraSinkConfiguration.DISABLE_QUERY_UNS, "true");

        // Add some spaces in between elements and values on purpose, these should be trimmed
        if (!CassandraSinkConfiguration.KEYSPACE.equals(propToExclude)) {
            conf.setProperty(CassandraSinkConfiguration.KEYSPACE, "testKeyspace");
        }

        if (!CassandraSinkConfiguration.TABLE_NAME.equals(propToExclude)) {
            conf.setProperty(CassandraSinkConfiguration.TABLE_NAME, "testTable");
        }

        if (!CassandraSinkConfiguration.CLUSTER_NAME.equals(propToExclude)) {
            conf.setProperty(CassandraSinkConfiguration.CLUSTER_NAME, "clusterFoo");
        }

        if (!CassandraSinkConfiguration.INITIAL_HOSTS.equals(propToExclude)) {
            conf.setProperty(CassandraSinkConfiguration.INITIAL_HOSTS, "localhost1,localhost2");
        }

        if (!CassandraSinkConfiguration.COLUMN_LIST.equals(propToExclude)) {
            conf.setProperty(CassandraSinkConfiguration.COLUMN_LIST, "f1,f2,f3,f4,f5");
        }

        if (!CassandraSinkConfiguration.PARTITION_KEYS.equals(propToExclude)) {
            conf.setProperty(CassandraSinkConfiguration.PARTITION_KEYS, "f1,f2");
        }

        if (!CassandraSinkConfiguration.CLUSTERING_KEYS.equals(propToExclude)) {
            conf.setProperty(CassandraSinkConfiguration.CLUSTERING_KEYS, "f3");
        }

        if (!CassandraSinkConfiguration.PARTITION_TYPE.equals(propToExclude)) {
            conf.setProperty(CassandraSinkConfiguration.PARTITION_TYPE, "normal");
        }

        if (!CassandraSinkConfiguration.DATACENTER.equals(propToExclude)) {
            conf.setProperty(CassandraSinkConfiguration.DATACENTER, "TEST_DC");
        }

        return conf;
    }
}
