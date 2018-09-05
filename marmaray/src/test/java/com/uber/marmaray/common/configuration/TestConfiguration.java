/*
 * Copyright (c) 2018 Uber Technologies, Inc.
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following conditions.
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
import com.uber.marmaray.common.util.AbstractSparkTest;
import java.io.File;
import java.io.InputStream;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestConfiguration extends AbstractSparkTest {

    public static final String CONFIG_YAML = "src/test/resources/config.yaml";
    private static final double DELTA = 1e-10;
    private InputStream scopeAwareConfigInputStream;

    @Before
    public void setup() {
        scopeAwareConfigInputStream = Configuration.class.getResourceAsStream("/configWithScopes.yaml");
    }

    @Test
    public void loadConfigurationYaml() {
        final Configuration conf = new Configuration(new File(CONFIG_YAML), Optional.absent());
        conf.setProperty("marmaray.string_configs.string3", "string3");

        Assert.assertEquals(1, conf.getIntProperty("marmaray.scalar_configs.integer", -1));
        Assert.assertEquals(1234567890123L, conf.getLongProperty("marmaray.scalar_configs.long", -1));
        Assert.assertEquals(1.23, conf.getDoubleProperty("marmaray.scalar_configs.double", -1.0), DELTA);
        Assert.assertEquals(true, conf.getBooleanProperty("marmaray.scalar_configs.boolean", false));
        Assert.assertEquals("string1", conf.getProperty("marmaray.string_configs.string1", "not string1"));
        Assert.assertEquals("string2", conf.getProperty("marmaray.string_configs.stringlist.string2").get());

        final Map<String, String> configMap = conf.getPropertiesWithPrefix("marmaray.string_configs.", true);
        Assert.assertEquals("string2", configMap.get("stringlist.string2"));
        Assert.assertEquals("string3", configMap.get("string3"));

        Assert.assertEquals(-1, conf.getIntProperty("marmaray.scalar_configs.notinteger", -1));
        Assert.assertEquals(-1, conf.getLongProperty("marmaray.scalar_configs.notlong", -1));
        Assert.assertEquals(-1.0, conf.getDoubleProperty("marmaray.scalar_configs.notdouble", -1.0), DELTA);
        Assert.assertEquals(false, conf.getBooleanProperty("marmaray.scalar_configs.notboolean", false));
        Assert.assertEquals("not string1", conf.getProperty("marmaray.string_configs.notstring1", "not string1"));
    }

    // Test happy path with config scope resolving
    @Test
    public void testConfigurationParseWithScope() {
        final Configuration conf = new Configuration(scopeAwareConfigInputStream, Optional.of("bootstrap"));
        Assert.assertEquals("jdbc", conf.getProperty("database.connection.type").get());
        Assert.assertEquals("5000", conf.getProperty("database.connection.port").get());
        Assert.assertEquals("4000", conf.getProperty("hbase.connection.port").get());
        Assert.assertEquals("1",
            conf.getProperty("marmaray.hoodie.tables.non_primitive_field_from_default.hello") .get());
        Assert.assertEquals("2",
            conf.getProperty("marmaray.hoodie.tables.non_primitive_field_from_default.hi").get());
        Assert.assertEquals("3",
            conf.getProperty("marmaray.hoodie.tables.non_primitive_field_from_bootstrap.heya") .get());
        Assert.assertEquals("1000",
            conf.getProperty("marmaray.hoodie.tables.target_table.parallelism").get());
        Assert.assertEquals("false",
            conf.getProperty("marmaray.hoodie.tables.target_table.combine_before_insert").get());
        Assert.assertEquals("2147483647",
            conf.getProperty("marmaray.hoodie.tables.target_table.parquet_max_file_size").get());

    }

    // Assert Preconditions failure since Configuration.SCOPE_OVERRIDE_MAPPING_KEY is not present in the config yaml
    @Test(expected = IllegalStateException.class)
    public void testConfigurationParseFailWithScopeButWithoutScopeOverrideKey() {
        final Configuration conf = new Configuration(new File(CONFIG_YAML),
            Optional.of("non-existent-scope"));
    }

    // Assert Preconditions failure since an invalid scope is passed
    @Test(expected = IllegalArgumentException.class)
    public void testConfigurationParseWithNonExistentScope() {
        new Configuration(scopeAwareConfigInputStream, Optional.of("non-existent-scope"));
    }
}
