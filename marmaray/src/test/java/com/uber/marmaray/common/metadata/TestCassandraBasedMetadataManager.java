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
package com.uber.marmaray.common.metadata;

import com.uber.marmaray.common.util.CassandraTestConstants;
import com.uber.marmaray.common.configuration.CassandraMetadataManagerConfiguration;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.configuration.MetadataManagerConfiguration;
import org.junit.After;
import org.junit.Test;
import org.junit.Before;
import org.apache.thrift.transport.TTransportException;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import org.junit.Assert;
import java.util.concurrent.atomic.AtomicBoolean;
import com.google.common.base.Optional;
import java.io.IOException;
import java.util.Map;
import com.uber.marmaray.common.util.CassandraTestUtil;


public class TestCassandraBasedMetadataManager  {

    private final static String JOB_NAME = "jobName";
    private CassandraBasedMetadataManager metadataManager;
    private Configuration conf = new Configuration();
    CassandraMetadataManagerConfiguration cassandraMetadataManagerConf;


    @Before
    public void setupTest() throws IOException {
        try {
            CassandraTestUtil.setupCluster();
        } catch (TTransportException | IOException e) {
            throw new JobRuntimeException("Error while initializing Cassandra cluster for test: " + e.getMessage());
        }
        this.conf.setProperty(MetadataManagerConfiguration.JOB_NAME, JOB_NAME);
        this.conf.setProperty(MetadataManagerConfiguration.TYPE, "CASSANDRA");
        this.conf.setProperty(CassandraMetadataManagerConfiguration.CLUSTER, "clusterName");
        this.conf.setProperty(CassandraMetadataManagerConfiguration.NATIVE_TRANSPORT_PORT, "9142");
        this.conf.setProperty(CassandraMetadataManagerConfiguration.INITIAL_HOSTS, CassandraTestConstants.LOCALHOST);
        this.conf.setProperty(CassandraMetadataManagerConfiguration.USERNAME, "marmaray");
        this.conf.setProperty(CassandraMetadataManagerConfiguration.PASSWORD, "password");
        this.conf.setProperty(CassandraMetadataManagerConfiguration.TABLE_NAME, "marmaray_metadata_table");
        this.conf.setProperty(CassandraMetadataManagerConfiguration.KEYSPACE, CassandraTestConstants.KEY_SPACE);
        this.cassandraMetadataManagerConf = new CassandraMetadataManagerConfiguration(this.conf);
        this.metadataManager = new CassandraBasedMetadataManager(cassandraMetadataManagerConf,
                new AtomicBoolean(true));
    }

    @After
    public void teardownTest() {
        CassandraTestUtil.teardownCluster();
    }

    @Test
    public void testReadWrite() throws IOException {
        this.metadataManager.set(MetadataConstants.CHECKPOINT_KEY, new StringValue("val"));
        final Optional<StringValue> readValue = this.metadataManager.get(MetadataConstants.CHECKPOINT_KEY);
        Assert.assertTrue(readValue.isPresent());
        Assert.assertTrue(readValue.get().getValue().equals("val"));

        // save metadata map back to Cassandra
        this.metadataManager.saveChanges();

        // reload the metadata from Cassandra and check
        final Map<String, StringValue> metadataMap = this.metadataManager.generateMetaDataMap();
        Assert.assertTrue(metadataMap.get(MetadataConstants.CHECKPOINT_KEY).getValue().equals("val"));
    }

    @Test
    public void testOverwriteCheckpointValue() throws IOException {

        final StringValue val1 = new StringValue("testVal");
        this.metadataManager.set(MetadataConstants.CHECKPOINT_KEY, val1);

        final StringValue val2 = new StringValue("testVal2");
        this.metadataManager.set(MetadataConstants.CHECKPOINT_KEY, val2);

        final Optional<StringValue> readValue = this.metadataManager.get(MetadataConstants.CHECKPOINT_KEY);
        Assert.assertTrue(readValue.isPresent());
        Assert.assertTrue(readValue.get().getValue().equals("testVal2"));

        this.metadataManager.saveChanges();

        final Map<String, StringValue> metaDataMap= this.metadataManager.generateMetaDataMap();
        Assert.assertTrue(metaDataMap.get(MetadataConstants.CHECKPOINT_KEY).getValue().equals("testVal2"));
    }


    @Test
    public void testDeletionIsPropagated() throws IOException {
        this.metadataManager.generateMetaDataMap();
        this.metadataManager.deleteAllMetadataOfJob(JOB_NAME);
        final Optional<StringValue> readValue = this.metadataManager.get(MetadataConstants.CHECKPOINT_KEY);
        Assert.assertFalse(readValue.isPresent());
    }
}