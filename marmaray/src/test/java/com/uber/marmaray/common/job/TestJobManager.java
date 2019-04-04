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

package com.uber.marmaray.common.job;

import com.google.common.base.Optional;
import com.uber.marmaray.TestSparkUtil;
import com.uber.marmaray.common.actions.IJobDagAction;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.configuration.LockManagerConfiguration;
import com.uber.marmaray.common.configuration.SparkConfiguration;
import com.uber.marmaray.common.configuration.ZookeeperConfiguration;
import com.uber.marmaray.common.metadata.JobManagerMetadataTracker;
import com.uber.marmaray.common.reporters.Reporters;
import com.uber.marmaray.common.spark.SparkArgs;
import com.uber.marmaray.common.spark.SparkFactory;
import com.uber.marmaray.common.status.BaseStatus;
import com.uber.marmaray.common.status.IStatus;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.curator.test.TestingServer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
@Slf4j
public class TestJobManager {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @NonNull
    private static final Configuration conf = new Configuration();

    @Mock
    private TestingServer mockZkServer;

    @Mock
    private JobDag mockJobDag1, mockJobDag2, mockJobDag3;

    @Mock
    private JobManagerMetadataTracker mockJobManagerMetatdataTracker;

    @Mock
    private IJobDagAction mockJobDagAction;

    @Before
    public void testSetup() throws Exception {
        doNothing().when(this.mockZkServer).start();
        conf.setProperty(LockManagerConfiguration.IS_ENABLED, "false");
        conf.setProperty(LockManagerConfiguration.ZK_BASE_PATH, "/////test////lock_manager////");
        conf.setProperty(ZookeeperConfiguration.ZK_QUORUM, "connect string");
        conf.setProperty(ZookeeperConfiguration.ZK_PORT, Integer.toString(2181));
        conf.setProperty(LockManagerConfiguration.ACQUIRE_LOCK_TIME_MS, Integer.toString(10 * 1000));
        conf.setProperty(LockManagerConfiguration.ZK_SESSION_TIMEOUT_MS, Integer.toString(30 * 1000));
        conf.setProperty(LockManagerConfiguration.ZK_CONNECTION_TIMEOUT_MS, Integer.toString(12 * 1000));
        JobManager.reset();
    }

    @Test
    public void testEmptyJobDagException() {
        final SparkArgs sparkArgs = getSampleMarmaraySparkArgs();
        final SparkFactory sparkFactory = new SparkFactory(sparkArgs);

        final JobManager jobManager = JobManager.createJobManager(this.conf,
                "test_app_name", "daily", sparkFactory, new Reporters());

        this.thrown.expect(RuntimeException.class);
        this.thrown.expectMessage("No job dags to execute");

        jobManager.run();
        sparkFactory.stop();
    }

    @Test
    public void testSuccessCase() {
        final SparkArgs sparkArgs = getSampleMarmaraySparkArgs();
        final SparkFactory sparkFactory = new SparkFactory(sparkArgs);

        final BaseStatus status = new BaseStatus();
        status.setStatus(IStatus.Status.SUCCESS);

        when(this.mockJobDag1.getDataFeedName()).thenReturn("data_feed_name1");
        when(this.mockJobDag1.getJobName()).thenReturn("job_name1");
        when(this.mockJobDag2.getDataFeedName()).thenReturn("data_feed_name2");
        when(this.mockJobDag2.getJobName()).thenReturn("job_name2");
        when(this.mockJobDag3.getDataFeedName()).thenReturn("data_feed_name3");
        when(this.mockJobDag3.getJobName()).thenReturn("job_name3");
        when(this.mockJobDag1.execute()).thenReturn(status);
        when(this.mockJobDag2.execute()).thenReturn(status);
        when(this.mockJobDag3.execute()).thenReturn(status);
        when(this.mockJobDag1.getJobManagerMetadata()).thenReturn(new HashMap<>());
        when(this.mockJobDag2.getJobManagerMetadata()).thenReturn(new HashMap<>());
        when(this.mockJobDag3.getJobManagerMetadata()).thenReturn(new HashMap<>());
        doNothing().when(this.mockJobManagerMetatdataTracker).writeJobManagerMetadata();

        final JobManager jobManager = JobManager.createJobManager(this.conf,
                "test_app_name", "daily", false, sparkFactory, new Reporters());

        jobManager.addJobDag(this.mockJobDag1);
        jobManager.addJobDags(Arrays.asList(this.mockJobDag2, this.mockJobDag3));
        jobManager.setJobManagerMetadataEnabled(true);
        jobManager.setTracker(this.mockJobManagerMetatdataTracker);
        jobManager.addPostJobManagerAction(this.mockJobDagAction);
        jobManager.addPostJobManagerActions(new ArrayList<>());
        jobManager.run();
        sparkFactory.stop();

        // verify
        verify(this.mockJobDag1, times(4)).getDataFeedName();
        verify(this.mockJobDag2, times(4)).getDataFeedName();
        verify(this.mockJobDag3, times(4)).getDataFeedName();
        verify(this.mockJobDag1, times(1)).execute();
        verify(this.mockJobDag2, times(1)).execute();
        verify(this.mockJobDag3, times(1)).execute();
        assertEquals("Test doesn't return successful status for jobdag1.", IStatus.Status.SUCCESS,
                jobManager.getJobManagerStatus().getJobStatuses().get("job_name1").getStatus());
        assertEquals("Test doesn't return successful status for jobdag2.", IStatus.Status.SUCCESS,
                jobManager.getJobManagerStatus().getJobStatuses().get("job_name2").getStatus());
        assertEquals("Test doesn't return successful status for jobdag3.", IStatus.Status.SUCCESS,
                jobManager.getJobManagerStatus().getJobStatuses().get("job_name3").getStatus());
        assertNotNull(jobManager.getJobMetrics());
        assertNotNull(jobManager.getReporters());
        assertNotNull(jobManager.getConf());
        assertNotNull(jobManager.getSparkFactory());
    }

    @Test
    public void testJobRunException() {
        final SparkArgs sparkArgs = getSampleMarmaraySparkArgs();
        final SparkFactory sparkFactory = new SparkFactory(sparkArgs);

        final BaseStatus status = new BaseStatus();
        status.setStatus(IStatus.Status.SUCCESS);

        when(this.mockJobDag1.getDataFeedName()).thenReturn("data_feed_name1");
        when(this.mockJobDag1.getJobName()).thenReturn("job_name1");
        when(this.mockJobDag1.execute()).thenThrow(new RuntimeException("Mock exception"));
        when(this.mockJobDag1.getJobManagerMetadata()).thenReturn(new HashMap<>());
        doNothing().when(this.mockJobManagerMetatdataTracker).writeJobManagerMetadata();

        final JobManager jobManager = JobManager.createJobManager(this.conf,
                "test_app_name", "daily", false, sparkFactory, new Reporters());

        jobManager.addJobDag(this.mockJobDag1);
        jobManager.setJobManagerMetadataEnabled(true);
        jobManager.setTracker(this.mockJobManagerMetatdataTracker);
        jobManager.addPostJobManagerAction(this.mockJobDagAction);
        jobManager.addPostJobManagerActions(new ArrayList<>());
        jobManager.run();
        sparkFactory.stop();

        // verify
        assertEquals("Test doesn't return FAILURE status.", IStatus.Status.FAILURE,
                jobManager.getJobManagerStatus().getStatus());
        assertEquals("Test should return only one exception.", 2,
                jobManager.getJobManagerStatus().getExceptions().size());
        assertTrue("Test doesn't return expected error message.",
                jobManager.getJobManagerStatus().getExceptions().get(0).getMessage().contains("Mock exception"));
    }

    private SparkArgs getSampleMarmaraySparkArgs() {
        final Schema recordSchema = SchemaBuilder.record("fooRecord").fields().name("abc").type()
                .intType().intDefault(0).endRecord();

        final Map<String, String> overrideSparkProperties = new HashMap<>();
        overrideSparkProperties.put("spark.master", "local[2]");
        overrideSparkProperties.put("spark.app.name", "foo_bar");

        final com.uber.marmaray.common.configuration.Configuration conf =
                new com.uber.marmaray.common.configuration.Configuration(
                        TestSparkUtil.class.getResourceAsStream("/config.yaml"),
                        Optional.absent());

        SparkConfiguration.overrideSparkConfInConfiguration(conf, overrideSparkProperties);
        return new SparkArgs(Arrays.asList(recordSchema), Collections.emptyList(), conf);
    }
}
