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

import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.configuration.HadoopConfiguration;
import com.uber.marmaray.common.configuration.HoodieConfiguration;
import com.uber.marmaray.common.util.AbstractSparkTest;
import com.uber.marmaray.common.util.FileTestUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.RecordBuilder;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Setting all the tests to ignore as the implementation of the job dag will require the
 * refactoring of HoodieBasedMetadataManager
 *
 * In addition, the metadata manager is dependent on Hoodie Client which requires a
 * non-backwards and old version of Google Guava (v.15).  Hoodie Client needs to be updated
 * to no longer use the Closeables.closeQuietly() method which has been deprecated.
 */
@Slf4j
public class TestHoodieBasedMetadataManager extends AbstractSparkTest {

    @Test
    public void testHoodieMetadataManager() throws IOException {
        final Path basePath = new Path(FileTestUtil.getTempFolder());
        final String tableName = "test-table";
        final String schemaStr = getSchema(10).toString();

        final Configuration conf = new Configuration();
        final HoodieConfiguration hoodieConf = HoodieConfiguration.newBuilder(conf, tableName).withTableName(tableName)
            .withBasePath(basePath.toString()).withSchema(schemaStr).withMetricsPrefix("hoodieMetricsPrefix")
            .enableMetrics(false).build();
        final AtomicBoolean condition = new AtomicBoolean(true);
        final HoodieBasedMetadataManager mgr =
            new HoodieBasedMetadataManager(hoodieConf, condition, this.jsc.get());

        // When no previous metadata is present then metadata map is expected to be empty.
        Assert.assertEquals(0, mgr.getAll().size());
        Assert.assertTrue(mgr.getMetadataInfo().get(HoodieBasedMetadataManager.HOODIE_METADATA_KEY).isEmpty());

        final String testKey = "partition1";
        final String testValue = "offset1";
        mgr.set(testKey, new StringValue(testValue));

        // Now metadata map should have got updated.
        Assert.assertEquals(1, mgr.getAll().size());
        Assert.assertFalse(mgr.getMetadataInfo().get(HoodieBasedMetadataManager.HOODIE_METADATA_KEY).isEmpty());

        // Let's reset condition flag; so that saveChanges will not create any metadata / save metadata map.
        condition.set(false);
        mgr.saveChanges();
        // No hoodie commit files should have been created.
        Assert.assertFalse(condition.get());
        Assert.assertTrue(
            new HoodieTableMetaClient(
                new HadoopConfiguration(hoodieConf.getConf()).getHadoopConf(), basePath.toString(), true)
                .getActiveTimeline().getCommitTimeline().filterCompletedInstants().empty());

        // Now let's enable metadata creation.
        condition.set(true);
        mgr.saveChanges();
        Assert.assertFalse(condition.get());
        Assert.assertFalse(
            new HoodieTableMetaClient(
                new HadoopConfiguration(hoodieConf.getConf()).getHadoopConf(), basePath.toString(), true)
                .getActiveTimeline().getCommitTimeline().filterCompletedInstants().empty());

        // Now let's create another
        final AtomicBoolean condition2 = new AtomicBoolean(true);
        final HoodieBasedMetadataManager mgr2 =
            new HoodieBasedMetadataManager(hoodieConf, condition2, this.jsc.get());
        Assert.assertEquals(1, mgr2.getAll().size());
        Assert.assertFalse(mgr2.getMetadataInfo().get(HoodieBasedMetadataManager.HOODIE_METADATA_KEY).isEmpty());
        Assert.assertEquals(mgr.getMetadataInfo(), mgr2.getMetadataInfo());
    }

    @Test
    public void testRemove() throws Exception {
        final Path basePath = new Path(FileTestUtil.getTempFolder());
        final String tableName = "test-table";
        final String schemaStr = getSchema(10).toString();

        final Configuration conf = new Configuration();
        final HoodieConfiguration hoodieConf = HoodieConfiguration.newBuilder(conf, tableName).withTableName(tableName)
            .withBasePath(basePath.toString()).withSchema(schemaStr).withMetricsPrefix("hoodieMetricsPrefix")
            .enableMetrics(false).build();
        final AtomicBoolean condition = new AtomicBoolean(true);
        final HoodieBasedMetadataManager mgr = new HoodieBasedMetadataManager(hoodieConf, condition, this.jsc.get());

        // set up default
        final String testKey = "partition1";
        final String testValue = "offset1";
        mgr.set(testKey, new StringValue(testValue));
        mgr.saveChanges();

        // mgr2 loads correctly
        final HoodieBasedMetadataManager mgr2 = new HoodieBasedMetadataManager(hoodieConf, condition, this.jsc.get());
        Assert.assertEquals(testValue, mgr2.get(testKey).get().getValue());
        mgr2.remove(testKey);
        Assert.assertFalse(mgr2.get(testKey).isPresent());

        // mgr2 hasn't saved yet, so should still get old value
        final HoodieBasedMetadataManager mgr3 = new HoodieBasedMetadataManager(hoodieConf, condition, this.jsc.get());
        Assert.assertEquals(testValue, mgr3.get(testKey).get().getValue());

        // save remove
        condition.set(true);
        mgr2.saveChanges();

        // new load shouldn't find it anymore
        final HoodieBasedMetadataManager mgr4 = new HoodieBasedMetadataManager(hoodieConf, condition, this.jsc.get());
        Assert.assertFalse(mgr4.get(testKey).isPresent());

    }

    private Schema getSchema(final int numOfSubFields) {
        final RecordBuilder<Schema> recordSchema = SchemaBuilder.builder().record("test");
        final FieldAssembler<Schema> fields = recordSchema.fields();
        for (int i = 0; i < numOfSubFields; i++) {
            fields.optionalString("test" + i);
        }
        return fields.endRecord();
    }
}
