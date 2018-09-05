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

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.validator.constraints.NotEmpty;
import org.junit.Assert;
import org.junit.Test;

import static com.uber.marmaray.common.util.SchemaTestUtil.getSchema;

@Slf4j
public class TestHoodieConfiguration {

    @Test
    public void testMultiTableConfiguration() {
        final String tableName = "table1";
        final String basePath = "/tmp/basePath";

        Assert.assertEquals("marmaray.hoodie.tables.table1.insert_split_size",
            HoodieConfiguration.getTablePropertyKey(HoodieConfiguration.HOODIE_INSERT_SPLIT_SIZE, tableName));
        Assert.assertEquals("marmaray.hoodie.default.insert_split_size",
            HoodieConfiguration.getDefaultPropertyKey(HoodieConfiguration.HOODIE_INSERT_SPLIT_SIZE));

        // Integer.
        verifyProperty(tableName, createHoodieConfiguration(tableName, basePath), new Integer(101), new Integer(280),
            new Integer(880));
        // Long.
        verifyProperty(tableName, createHoodieConfiguration(tableName, basePath), new Long(101), new Long(280),
            new Long(880));
        // Double.
        verifyProperty(tableName, createHoodieConfiguration(tableName, basePath), new Double(10.1), new Double(2.80),
            new Double(8.80));
        // Boolean.
        verifyProperty(tableName, createHoodieConfiguration(tableName, basePath), new Boolean(true), new Boolean(false),
            new Boolean(false));
        // String.
        verifyProperty(tableName, createHoodieConfiguration(tableName, basePath), new Integer("101"),
            new Integer("280"), new Integer("880"));
    }

    public static HoodieConfiguration createHoodieConfiguration(@NotEmpty final String tableName,
        @NotEmpty final String basePath) {
        final String schemaStr = getSchema("ts", "uuid", 4, 8).toString();
        return HoodieConfiguration
            .newBuilder(tableName)
            .withTableName(tableName)
            .withBasePath(basePath)
            .withSchema(schemaStr)
            .enableMetrics(false)
            .build();
    }

    private static <T> void verifyProperty(@NotEmpty final String tableName,
        @NonNull final HoodieConfiguration hoodieConf, @NonNull final T defaultValue,
        @NonNull final T defaultPropertyValue, @NonNull final T tableValue) {
        Object value =
            hoodieConf.getProperty(HoodieConfiguration.HOODIE_INSERT_SPLIT_SIZE, defaultValue);
        Assert.assertTrue(value.equals(defaultValue) && value.getClass() == defaultValue.getClass());
        hoodieConf.getConf().setProperty(
            HoodieConfiguration.getDefaultPropertyKey(HoodieConfiguration.HOODIE_INSERT_SPLIT_SIZE),
            defaultPropertyValue.toString());
        value =
            hoodieConf.getProperty(HoodieConfiguration.HOODIE_INSERT_SPLIT_SIZE, defaultValue);
        Assert.assertTrue(
            value.equals(defaultPropertyValue) && value.getClass() == defaultPropertyValue.getClass());
        hoodieConf.getConf().setProperty(
            HoodieConfiguration.getTablePropertyKey(HoodieConfiguration.HOODIE_INSERT_SPLIT_SIZE,
                tableName), tableValue.toString());
        value =
            hoodieConf.getProperty(HoodieConfiguration.HOODIE_INSERT_SPLIT_SIZE, defaultValue);
        Assert.assertTrue(
            value.equals(tableValue) && value.getClass() == tableValue.getClass());
    }
}
