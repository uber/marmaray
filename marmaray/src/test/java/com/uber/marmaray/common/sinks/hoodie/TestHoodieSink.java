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
package com.uber.marmaray.common.sinks.hoodie;

import com.google.common.annotations.VisibleForTesting;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.marmaray.common.AvroPayload;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.configuration.HoodieConfiguration;
import com.uber.marmaray.common.converters.data.HoodieSinkDataConverter;
import com.uber.marmaray.common.converters.data.TSBasedHoodieSinkDataConverter;
import com.uber.marmaray.common.metadata.HoodieBasedMetadataManager;
import com.uber.marmaray.common.metadata.IMetadataManager;
import com.uber.marmaray.common.metadata.MemoryMetadataManager;
import com.uber.marmaray.common.metadata.NoOpMetadataManager;
import com.uber.marmaray.common.metadata.StringValue;
import com.uber.marmaray.common.metrics.DataFeedMetrics;
import com.uber.marmaray.common.metrics.Metric;
import com.uber.marmaray.common.sinks.hoodie.HoodieSink.HoodieWriteClientWrapper;
import com.uber.marmaray.common.util.AbstractSparkTest;
import com.uber.marmaray.common.util.FileTestUtil;
import com.uber.marmaray.utilities.FSUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.uber.marmaray.common.sinks.hoodie.HoodieSink.HoodieSinkOp.BULK_INSERT;
import static com.uber.marmaray.common.sinks.hoodie.HoodieSink.HoodieSinkOp.DEDUP_INSERT;
import static com.uber.marmaray.common.sinks.hoodie.HoodieSink.HoodieSinkOp.INSERT;
import static com.uber.marmaray.common.sinks.hoodie.HoodieSink.HoodieSinkOp.UPSERT;
import static com.uber.marmaray.common.util.SchemaTestUtil.getRandomData;
import static com.uber.marmaray.common.util.SchemaTestUtil.getSchema;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * This is needed to add spy to HoodieWriteClientWrapper.
 */
class MockHoodieSink extends HoodieSink {

    @VisibleForTesting
    @Getter
    private HoodieWriteClientWrapper mockWriteClient;

    public MockHoodieSink(@NonNull final HoodieConfiguration hoodieConf,
        @NonNull final HoodieSinkDataConverter hoodieKeyGenerator, @NonNull final JavaSparkContext jsc,
        @NonNull final HoodieSinkOp op) {
        super(hoodieConf, hoodieKeyGenerator, jsc, op, new MemoryMetadataManager());
    }

    public MockHoodieSink(@NonNull final HoodieConfiguration hoodieConf,
        @NonNull final HoodieSinkDataConverter hoodieKeyGenerator, @NonNull final JavaSparkContext jsc,
        @NonNull final HoodieSinkOp op,
        @NonNull final IMetadataManager metadataMgr) {
        super(hoodieConf, hoodieKeyGenerator, jsc, op, metadataMgr);
    }

    @Override
    protected HoodieWriteClientWrapper getHoodieWriteClient(
        @NonNull final HoodieWriteConfig hoodieWriteConfig) {
        this.mockWriteClient = spy(super.getHoodieWriteClient(hoodieWriteConfig));
        return this.mockWriteClient;
    }
}

@Slf4j
public class TestHoodieSink extends AbstractSparkTest {

    private static final String TS_KEY = "timestamp";
    private static final String RECORD_KEY = "primaryKey";
    private final Configuration conf = new Configuration();

    // TODO(T933933) need to add tests for Error Records.

    @Test
    public void testUpdateInsertParallelism() {
        final String basePath = "/basePath";
        final String tableName = "test-table";
        final String schemaStr = getSchema("TS", "RECORD_KEY", 4, 8).toString();
        final HoodieConfiguration hoodieConf =
            HoodieConfiguration.newBuilder(tableName).withTableName(tableName).withMetricsPrefix("test")
                .withBasePath(basePath).withSchema(schemaStr).enableMetrics(false).build();
        final HoodieSink mockSink =
            spy(new HoodieSink(hoodieConf, mock(HoodieSinkDataConverter.class),
                mock(JavaSparkContext.class), HoodieSink.HoodieSinkOp.NO_OP, new NoOpMetadataManager()));
        when(mockSink.calculateNewBulkInsertParallelism(anyLong())).thenReturn(18);
        Assert.assertTrue(mockSink.updateInsertParallelism(1000));
        Assert.assertEquals(18, hoodieConf.getInsertParallelism());
        Assert.assertEquals(HoodieConfiguration.DEFAULT_HOODIE_PARALLELISM, hoodieConf.getBulkInsertParallelism());
    }

    @Test
    public void testUpdateBulkInsertParallelism() {
        final String basePath = "/basePath";
        final String tableName = "test-table";
        final String schemaStr = getSchema("TS", "RECORD_KEY", 4, 8).toString();
        final HoodieConfiguration hoodieConf =
            HoodieConfiguration.newBuilder(tableName).withTableName(tableName).withMetricsPrefix("test")
                .withBasePath(basePath).withSchema(schemaStr).enableMetrics(false).build();
        final HoodieSink mockSink =
            spy(new HoodieSink(hoodieConf, mock(HoodieSinkDataConverter.class),
                mock(JavaSparkContext.class), HoodieSink.HoodieSinkOp.NO_OP, new NoOpMetadataManager()));
        when(mockSink.calculateNewBulkInsertParallelism(anyLong())).thenReturn(18);
        Assert.assertTrue(mockSink.updateBulkInsertParallelism(1000));
        Assert.assertEquals(18, hoodieConf.getBulkInsertParallelism());
        Assert.assertEquals(HoodieConfiguration.DEFAULT_HOODIE_PARALLELISM, hoodieConf.getInsertParallelism());
    }

    @Test
    public void testHoodieSinkWriteInsertWithoutMetadata() throws IOException {
        final String basePath = FileTestUtil.getTempFolder();
        final String tableName = "test-table";
        final String schemaStr = getSchema(TS_KEY, RECORD_KEY, 4, 8).toString();
        final HoodieSinkDataConverter hoodieKeyGenerator =
            new TSBasedHoodieSinkDataConverter(conf, RECORD_KEY, TS_KEY, TimeUnit.MILLISECONDS);
        final HoodieConfiguration hoodieConf =
            HoodieConfiguration.newBuilder(tableName).withTableName(tableName).withMetricsPrefix("test")
                .withBasePath(basePath).withSchema(schemaStr).enableMetrics(false).build();
        final MockHoodieSink hoodieSink = new MockHoodieSink(hoodieConf, hoodieKeyGenerator, jsc.get(), INSERT);
        final JavaRDD<AvroPayload> inputRDD =
            this.jsc.get().parallelize(getRandomData(schemaStr, TS_KEY, RECORD_KEY, 10));

        // Trying to write as an INSERT without HoodieBasedMetadataManager.
        hoodieSink.write(inputRDD);
        final HoodieWriteClientWrapper hoodieWriteClientWrapper = hoodieSink.getMockWriteClient();

        // It should generate exactly one commit file.
        Assert.assertEquals(1, getCommitFiles(basePath, FSUtils.getFs(new Configuration())).size());
        /*
            Expected function calls.
            1) startCommit (once).
            2) upsert (zero times).
            3) insert (once).
            4) commit (once with empty metadata).
            5) close (once).
            6) bulkInsert (zero).
         */
        Mockito.verify(hoodieWriteClientWrapper, Mockito.times(1)).startCommit();
        Mockito.verify(hoodieWriteClientWrapper, Mockito.times(0))
            .upsert(Matchers.any(JavaRDD.class), Matchers.anyString());
        Mockito.verify(hoodieWriteClientWrapper, Mockito.times(1))
            .insert(Matchers.any(JavaRDD.class), Matchers.anyString());
        Mockito.verify(hoodieWriteClientWrapper, Mockito.times(1))
            .commit(Matchers.anyString(), Matchers.any(JavaRDD.class),
                Matchers.same(java.util.Optional.empty()));
        Mockito.verify(hoodieWriteClientWrapper, Mockito.times(1)).close();
        Mockito.verify(hoodieWriteClientWrapper, Mockito.times(0))
            .bulkInsert(Matchers.any(JavaRDD.class), Matchers.anyString());
    }

    @Test
    public void testHoodieSinkWriteUpsertWithoutMetadata() throws IOException {
        final String basePath = FileTestUtil.getTempFolder();
        final String tableName = "test-table";
        final String schemaStr = getSchema(TS_KEY, RECORD_KEY, 4, 8).toString();
        final HoodieSinkDataConverter hoodieKeyGenerator =
            new TSBasedHoodieSinkDataConverter(this.conf, RECORD_KEY, TS_KEY, TimeUnit.MILLISECONDS);
        final HoodieConfiguration hoodieConf =
            HoodieConfiguration.newBuilder(tableName).withTableName(tableName).withMetricsPrefix("test")
                .withBasePath(basePath).withSchema(schemaStr).enableMetrics(false).build();
        final MockHoodieSink hoodieSink = new MockHoodieSink(hoodieConf, hoodieKeyGenerator, jsc.get(), UPSERT);
        final JavaRDD<AvroPayload> inputRDD =
            this.jsc.get().parallelize(getRandomData(schemaStr, TS_KEY, RECORD_KEY, 10));

        // Trying to write as an UPSERT without HoodieBasedMetadataManager.
        hoodieSink.write(inputRDD);
        final HoodieWriteClientWrapper hoodieWriteClientWrapper = hoodieSink.getMockWriteClient();
        // It should generate exactly one commit file.
        Assert.assertEquals(1, getCommitFiles(basePath, FSUtils.getFs(new Configuration())).size());
        /*
            Expected function calls.
            1) startCommit (once).
            2) upsert (once).
            3) bulkInsert (zero times).
            4) commit (once with empty metadata).
            5) close (once).
            6) insert (zero times).
         */

        Mockito.verify(hoodieWriteClientWrapper, Mockito.times(1)).startCommit();
        Mockito.verify(hoodieWriteClientWrapper, Mockito.times(1))
            .upsert(Matchers.any(JavaRDD.class), Matchers.anyString());
        Mockito.verify(hoodieWriteClientWrapper, Mockito.times(0))
            .bulkInsert(Matchers.any(JavaRDD.class), Matchers.anyString());
        Mockito.verify(hoodieWriteClientWrapper, Mockito.times(1))
            .commit(Matchers.anyString(), Matchers.any(JavaRDD.class),
                Matchers.same(java.util.Optional.empty()));
    }

    @Test
    public void testHoodieSinkWriteInsertWithMetadata() throws IOException {
        final String basePath = FileTestUtil.getTempFolder();
        final String tableName = "test-table";
        final String schemaStr = getSchema(TS_KEY, RECORD_KEY, 4, 8).toString();
        final HoodieSinkDataConverter hoodieKeyGenerator =
            new TSBasedHoodieSinkDataConverter(this.conf, RECORD_KEY, TS_KEY, TimeUnit.MILLISECONDS);
        final HoodieConfiguration hoodieConf =
            HoodieConfiguration.newBuilder(tableName).withTableName(tableName).withMetricsPrefix("test")
                .withBasePath(basePath).withSchema(schemaStr).enableMetrics(false).build();
        final HoodieBasedMetadataManager hoodieBasedMetadataManager = new HoodieBasedMetadataManager(hoodieConf,
            new AtomicBoolean(true), this.jsc.get());
        hoodieBasedMetadataManager.set("randomKey", new StringValue("randomValue"));
        final MockHoodieSink hoodieSink = new MockHoodieSink(hoodieConf, hoodieKeyGenerator, jsc.get(), INSERT,
            hoodieBasedMetadataManager);
        final JavaRDD<AvroPayload> inputRDD =
            this.jsc.get().parallelize(getRandomData(schemaStr, TS_KEY, RECORD_KEY, 10));

        // Trying to write as an INSERT with HoodieBasedMetadataManager.
        hoodieSink.write(inputRDD);
        final HoodieWriteClientWrapper hoodieWriteClientWrapper = hoodieSink.getMockWriteClient();

        // It should generate exactly one commit file.
        Assert.assertEquals(1, getCommitFiles(basePath, FSUtils.getFs(new Configuration())).size());
        /*
            Expected function calls.
            1) startCommit (once).
            2) upsert (zero times).
            3) insert (once).
            4) commit (once with metadata).
            5) close (once).
            It should also reset flag for metadata manager.
         */
        Mockito.verify(hoodieWriteClientWrapper, Mockito.times(1)).startCommit();
        Mockito.verify(hoodieWriteClientWrapper, Mockito.times(0))
            .upsert(Matchers.any(JavaRDD.class), Matchers.anyString());
        Mockito.verify(hoodieWriteClientWrapper, Mockito.times(1))
            .insert(Matchers.any(JavaRDD.class), Matchers.anyString());
        Mockito.verify(hoodieWriteClientWrapper, Mockito.times(1))
            .commit(Matchers.anyString(), Matchers.any(JavaRDD.class),
                Matchers.eq(java.util.Optional.of(hoodieBasedMetadataManager.getMetadataInfo())));
        Mockito.verify(hoodieWriteClientWrapper, Mockito.times(1)).close();
        Assert.assertFalse(hoodieBasedMetadataManager.shouldSaveChanges().get());
    }

    @Test
    public void testHoodieSinkWriteUpsertWithMetadata() throws IOException {
        final String basePath = FileTestUtil.getTempFolder();
        final String tableName = "test-table";
        final String schemaStr = getSchema(TS_KEY, RECORD_KEY, 4, 8).toString();
        final HoodieSinkDataConverter hoodieKeyGenerator =
            new TSBasedHoodieSinkDataConverter(this.conf, RECORD_KEY, TS_KEY, TimeUnit.MILLISECONDS);
        final HoodieConfiguration hoodieConf =
            HoodieConfiguration.newBuilder(tableName).withTableName(tableName).withMetricsPrefix("test")
                .withBasePath(basePath).withSchema(schemaStr).enableMetrics(false).build();
        final HoodieBasedMetadataManager hoodieBasedMetadataManager = new HoodieBasedMetadataManager(hoodieConf,
            new AtomicBoolean(true), this.jsc.get());
        hoodieBasedMetadataManager.set("randomKey", new StringValue("randomValue"));
        final MockHoodieSink hoodieSink = new MockHoodieSink(hoodieConf, hoodieKeyGenerator, jsc.get(), UPSERT,
            hoodieBasedMetadataManager);
        final JavaRDD<AvroPayload> inputRDD =
            this.jsc.get().parallelize(getRandomData(schemaStr, TS_KEY, RECORD_KEY, 10));

        // Trying to write as an UPSERT with HoodieBasedMetadataManager.
        hoodieSink.write(inputRDD);
        final HoodieWriteClientWrapper hoodieWriteClientWrapper = hoodieSink.getMockWriteClient();
        // It should generate exactly one commit file.
        Assert.assertEquals(1, getCommitFiles(basePath, FSUtils.getFs(new Configuration())).size());
        /*
            Expected function calls.
            1) startCommit (once).
            2) upsert (once).
            3) bulkInsert (zero times).
            4) commit (once with metadata).
            5) close (once).
            It should also reset flag for metadata manager.
         */

        Mockito.verify(hoodieWriteClientWrapper, Mockito.times(1)).startCommit();
        Mockito.verify(hoodieWriteClientWrapper, Mockito.times(1))
            .upsert(Matchers.any(JavaRDD.class), Matchers.anyString());
        Mockito.verify(hoodieWriteClientWrapper, Mockito.times(0))
            .bulkInsert(Matchers.any(JavaRDD.class), Matchers.anyString());
        Mockito.verify(hoodieWriteClientWrapper, Mockito.times(1))
            .commit(Matchers.anyString(), Matchers.any(JavaRDD.class),
                Matchers.eq(java.util.Optional.of(hoodieBasedMetadataManager.getMetadataInfo())));
        Assert.assertFalse(hoodieBasedMetadataManager.shouldSaveChanges().get());
    }

    @Test
    public void testHoodieSinkWriteDedupeInsert() throws IOException {
        final String basePath = FileTestUtil.getTempFolder();
        final String tableName = "test-table";
        final String schemaStr = getSchema(TS_KEY, RECORD_KEY, 4, 8).toString();
        final HoodieSinkDataConverter hoodieKeyGenerator =
            new TSBasedHoodieSinkDataConverter(this.conf, RECORD_KEY, TS_KEY, TimeUnit.MILLISECONDS);
        final HoodieConfiguration hoodieConf =
            HoodieConfiguration.newBuilder(tableName).withTableName(tableName).withMetricsPrefix("test")
                .withBasePath(basePath).withSchema(schemaStr)
                .withCombineBeforeInsert(true).withCombineBeforeUpsert(true).enableMetrics(false).build();
        final JavaRDD<AvroPayload> inputRDD =
            this.jsc.get().parallelize(getRandomData(schemaStr, TS_KEY, RECORD_KEY, 10));

        final MockHoodieSink hoodieSink = new MockHoodieSink(hoodieConf, hoodieKeyGenerator, jsc.get(), DEDUP_INSERT);
        // Trying to write as a DEDUP_INSERT without HoodieBasedMetadataManager.
        hoodieSink.write(inputRDD);
        final HoodieWriteClientWrapper hoodieWriteClientWrapper = hoodieSink.getMockWriteClient();

        // It should generate exactly one commit file.
        Assert.assertEquals(1, getCommitFiles(basePath, FSUtils.getFs(new Configuration())).size());
        /*
            Expected function calls.
            1) startCommit (once).
            2) upsert (zero times).
            3) insert (once).
            4) filter exists (once) - needed for dedup.
            4) commit (once with empty metadata).
            5) close (once).
         */
        Mockito.verify(hoodieWriteClientWrapper, Mockito.times(1)).startCommit();
        Mockito.verify(hoodieWriteClientWrapper, Mockito.times(0))
            .upsert(Matchers.any(JavaRDD.class), Matchers.anyString());
        Mockito.verify(hoodieWriteClientWrapper, Mockito.times(1))
            .insert(Matchers.any(JavaRDD.class), Matchers.anyString());
        Mockito.verify(hoodieWriteClientWrapper, Mockito.times(1))
            .filterExists(Matchers.any(JavaRDD.class));
        Mockito.verify(hoodieWriteClientWrapper, Mockito.times(1))
            .commit(Matchers.anyString(), Matchers.any(JavaRDD.class),
                Matchers.same(java.util.Optional.empty()));
        Mockito.verify(hoodieWriteClientWrapper, Mockito.times(1)).close();

        // If we try to re-insert then it should find all the records as a a part filterExists test and should not
        // call bulkInsert.
        final MockHoodieSink hoodieSink2 = new MockHoodieSink(hoodieConf, hoodieKeyGenerator, jsc.get(), DEDUP_INSERT);
        hoodieSink.write(inputRDD);
        final HoodieWriteClientWrapper hoodieWriteClientWrapper2 = hoodieSink.getMockWriteClient();

        Assert.assertEquals(2, getCommitFiles(basePath, FSUtils.getFs(new Configuration())).size());
        final ArgumentCaptor<JavaRDD> rddCaputure = ArgumentCaptor.forClass(JavaRDD.class);
        verify(hoodieWriteClientWrapper2).insert(rddCaputure.capture(), Matchers.any());
        // All records should get filtered out.
        Assert.assertEquals(0, rddCaputure.getValue().count());
    }

    @Test
    public void testHoodieSinkMetrics() throws IOException {
        final String basePath = FileTestUtil.getTempFolder();
        final String tableName = "test-table";
        final String schemaStr = getSchema(TS_KEY, RECORD_KEY, 4, 8).toString();
        final String brokenSchemaStr = getSchema(TS_KEY, RECORD_KEY, 0, 0).toString();
        final HoodieSinkDataConverter hoodieKeyGenerator =
                new TSBasedHoodieSinkDataConverter(conf, RECORD_KEY, TS_KEY, TimeUnit.MILLISECONDS);
        final HoodieConfiguration hoodieConf =
                HoodieConfiguration.newBuilder(tableName).withTableName(tableName).withMetricsPrefix("test")
                        .withBasePath(basePath).withSchema(schemaStr).enableMetrics(false).build();
        final MockHoodieSink hoodieSink = new MockHoodieSink(hoodieConf, hoodieKeyGenerator, jsc.get(), INSERT);
        final Map<String, String> emptyTags = new HashMap<>();
        final DataFeedMetrics dfm = new DataFeedMetrics(JOB_NAME, emptyTags);
        hoodieSink.setDataFeedMetrics(dfm);
        final Integer successRecordCount = 10;
        final Integer failedRecordCount = 13;
        final List<AvroPayload> data = getRandomData(schemaStr, TS_KEY, RECORD_KEY, successRecordCount);
        data.addAll(getRandomData(brokenSchemaStr, TS_KEY, RECORD_KEY, failedRecordCount));
        final JavaRDD<AvroPayload> inputRDD =
                this.jsc.get().parallelize(data);
        hoodieSink.write(inputRDD);

        final Set<Metric> ms = dfm.getMetricSet();
        final Map<String, Long> expected = ArrayUtils.<String, Long>toMap(
                new Object[][] {
                        {"output_rowcount", successRecordCount.longValue()},
                        {"error_rowcount", failedRecordCount.longValue()}
                });
        Assert.assertEquals(expected.size(), ms.size());
        ms.forEach( metric -> {
            final String key = metric.getMetricName();
            Assert.assertEquals("failure for metric " + key, expected.get(key), metric.getMetricValue());
        });
    }

    @Test
    public void testUserDefinedCommitTime() throws IOException {
        final String basePath = FileTestUtil.getTempFolder();
        final String tableName = "test-table";
        final String schemaStr = getSchema(TS_KEY, RECORD_KEY, 4, 8).toString();
        final HoodieSinkDataConverter hoodieKeyGenerator =
            new TSBasedHoodieSinkDataConverter(this.conf, RECORD_KEY, TS_KEY, TimeUnit.MILLISECONDS);
        final HoodieConfiguration hoodieConf =
            HoodieConfiguration.newBuilder(tableName).withTableName(tableName).withMetricsPrefix("test")
                .withBasePath(basePath).withSchema(schemaStr).enableMetrics(false).build();
        final JavaRDD<AvroPayload> inputRDD =
            this.jsc.get().parallelize(getRandomData(schemaStr, TS_KEY, RECORD_KEY, 10));

        final MockHoodieSink hoodieSink1 = new MockHoodieSink(hoodieConf, hoodieKeyGenerator, jsc.get(), BULK_INSERT);
        hoodieSink1.write(inputRDD);

        final HoodieWriteClientWrapper hoodieWriteClientWrapper1 = hoodieSink1.getMockWriteClient();
        Mockito.verify(hoodieWriteClientWrapper1, Mockito.times(1)).startCommit();

        final List<String> commitFilesAfterFirstCommit = getCommitFiles(basePath, FSUtils.getFs(new Configuration()));
        Assert.assertEquals(1, commitFilesAfterFirstCommit.size());

        final String customCommit =
            HoodieActiveTimeline.COMMIT_FORMATTER.format(
                new Date(new Date().getTime() - TimeUnit.DAYS.toMillis(365)));

        final MockHoodieSink hoodieSink2 = new MockHoodieSink(hoodieConf, hoodieKeyGenerator, jsc.get(), BULK_INSERT);
        hoodieSink2.setCommitTime(com.google.common.base.Optional.of(customCommit));

        hoodieSink2.write(inputRDD);
        final HoodieWriteClientWrapper hoodieWriteClientWrapper2 = hoodieSink2.getMockWriteClient();
        Mockito.verify(hoodieWriteClientWrapper2, Mockito.times(0)).startCommit();

        final List<String> commitFilesAfterSecondCommit = getCommitFiles(basePath, FSUtils.getFs(new Configuration()));
        Assert.assertEquals(2, commitFilesAfterSecondCommit.size());

        final String oldCommitTime = commitFilesAfterFirstCommit.get(0);
        commitFilesAfterSecondCommit.removeAll(commitFilesAfterFirstCommit);
        Assert.assertEquals(1, commitFilesAfterSecondCommit.size());
        final String newCommitTime = commitFilesAfterSecondCommit.get(0);
        Assert.assertEquals(customCommit, newCommitTime.split(".commit")[0]);
    }

    private List<String> getCommitFiles(final String basePath, final FileSystem fs) throws IOException {
        final List<String> commitFiles = new ArrayList<>();
        final RemoteIterator<LocatedFileStatus> filesI = fs.listFiles(new Path(basePath), true);
        while (filesI.hasNext()) {
            final LocatedFileStatus fileStatus = filesI.next();
            if (fileStatus.isFile() && fileStatus.getPath().getName().endsWith("commit")) {
                commitFiles.add(fileStatus.getPath().getName());
            }
        }
        return commitFiles;
    }
}
