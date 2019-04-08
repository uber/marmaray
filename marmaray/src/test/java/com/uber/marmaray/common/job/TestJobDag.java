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

import com.uber.marmaray.common.AvroPayload;
import com.uber.marmaray.common.actions.IJobDagAction;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.metadata.IMetadataManager;
import com.uber.marmaray.common.metrics.JobMetrics;
import com.uber.marmaray.common.reporters.IReporter;
import com.uber.marmaray.common.reporters.Reporters;
import com.uber.marmaray.common.sinks.ISink;
import com.uber.marmaray.common.sources.ISource;
import com.uber.marmaray.common.sources.IWorkUnitCalculator;
import com.uber.marmaray.common.status.BaseStatus;
import com.uber.marmaray.common.status.IStatus;
import com.uber.marmaray.common.util.AbstractSparkTest;
import org.apache.spark.api.java.JavaRDD;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.HashMap;

import static com.uber.marmaray.common.util.SchemaTestUtil.getRandomData;
import static com.uber.marmaray.common.util.SchemaTestUtil.getSchema;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestJobDag extends AbstractSparkTest {

    @Mock
    private ISource mockSource;

    @Mock
    private ISink mockSink;

    @Mock
    private IMetadataManager mockMetatdataManager;

    @Mock
    private IWorkUnitCalculator mockIWorkUnitCalculator;

    @Mock
    private IWorkUnitCalculator.IWorkUnitCalculatorResult mockIWorkUnitCalculatorResult;

    @Mock
    private JobMetrics mockJobMetrics;

    private Reporters reporters;

    @Mock
    private IReporter mockIReporter;

    @Mock
    private IJobDagAction mockJobDagAction;

    private static final String TS_KEY = "timestamp";
    private static final String RECORD_KEY = "primaryKey";

    @Before
    public void testSetup() {
        this.reporters = new Reporters();
        this.reporters.addReporter(this.mockIReporter);
        ThreadPoolService.init(new Configuration());
    }

    @After
    public void tearDown() {
        ThreadPoolService.shutdown(true);
    }

    @Test
    public void testSuccessCase() {
        final BaseStatus status = new BaseStatus();
        status.setStatus(IStatus.Status.SUCCESS);

        when(this.mockIWorkUnitCalculatorResult.getStatus()).thenReturn(status);
        when(this.mockIWorkUnitCalculator.computeWorkUnits()).thenReturn(this.mockIWorkUnitCalculatorResult);
        when(this.mockIWorkUnitCalculatorResult.hasWorkUnits()).thenReturn(true);

        // Spark JavaRDD mock
        final String schemaStr = getSchema(TS_KEY, RECORD_KEY, 4, 8).toString();
        final JavaRDD<AvroPayload> inputRDD =
                this.jsc.isPresent() ? this.jsc.get().parallelize(getRandomData(schemaStr, TS_KEY,
                        RECORD_KEY, 10)) : null;
        when(this.mockSource.getData(any())).thenReturn(inputRDD);

        final Dag jdag = new JobDag(this.mockSource, this.mockSink, this.mockMetatdataManager,
                this.mockIWorkUnitCalculator, "test_job_name",
                "test_data_feed_name", this.mockJobMetrics, this.reporters);
        ((JobDag) jdag).addAction(this.mockJobDagAction);
        jdag.setJobManagerMetadata(new HashMap<>());

        // Verify result is expected
        final IStatus res = jdag.execute();
        assertEquals("Job status doesn't return SUCCESS", IStatus.Status.SUCCESS, res.getStatus());
        assertNotNull(((JobDag) jdag).getDataFeedMetrics());
        assertEquals("DataFeedMetric doesn't return expected job name.", "test_job_name",
                ((JobDag) jdag).getDataFeedMetrics().getJobName());
    }

    @Test
    public void testMetadataManagerException() throws Exception {
        final BaseStatus status = new BaseStatus();
        status.setStatus(IStatus.Status.SUCCESS);
        final String exceptionMsg = "Mock metadataManager exception.";

        when(this.mockIWorkUnitCalculatorResult.getStatus()).thenReturn(status);
        when(this.mockIWorkUnitCalculator.computeWorkUnits()).thenReturn(this.mockIWorkUnitCalculatorResult);
        when(this.mockIWorkUnitCalculatorResult.hasWorkUnits()).thenReturn(false);
        // Throw IO exception in metadataManager
        doThrow(new IOException(exceptionMsg)).when(this.mockMetatdataManager).saveChanges();

        final Dag jdag = new JobDag(this.mockSource, this.mockSink, this.mockMetatdataManager,
                this.mockIWorkUnitCalculator, "test_job_name",
                "test_data_feed_name", this.mockJobMetrics, this.reporters);
        ((JobDag) jdag).addAction(this.mockJobDagAction);
        
        // Verify result is expected
        final IStatus res = jdag.execute();
        assertEquals("Job status doesn't return FAILURE", IStatus.Status.FAILURE, res.getStatus());
        assertEquals("Job status has more than one exception.", 1, res.getExceptions().size());
        assertEquals("Job status exception doesn't return expected message.", "Failed to save metadata changes " + exceptionMsg,
                res.getExceptions().get(0).getMessage());

    }
}
