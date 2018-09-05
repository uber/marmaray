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

import com.google.common.base.Optional;
import com.uber.hoodie.config.HoodieIndexConfig;
import lombok.NonNull;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.tools.ant.filters.StringInputStream;
import org.hibernate.validator.constraints.NotEmpty;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Properties;

import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(ConnectionFactory.class)
public class TestHoodieIndexConfiguration {

    private static final String TABLE_KEY = "test_table";

    @Test
    public void configureHoodieIndex() throws Exception {
        final Configuration conf = new Configuration(new StringInputStream("a: b"), Optional.absent());
        setHoodieTableProperty(conf, "table_name", "myTestTable");
        setHoodieTableProperty(conf, "base_path", "/path/to/my/test/table");

        // should use a bloom index
        Assert.assertEquals("BLOOM", getHoodiePropertyFromConfig(conf, "hoodie.index.type"));

        // test hbase index
        mockStatic(ConnectionFactory.class);
        final Connection mockConn = mock(Connection.class);
        final Admin mockAdmin = mock(Admin.class);
        when(ConnectionFactory.createConnection(Matchers.any())).thenReturn(mockConn);
        when(mockConn.getAdmin()).thenReturn(mockAdmin);
        when(mockAdmin.tableExists(Matchers.any())).thenReturn(true);
        setHoodieTableProperty(conf, "index.type", "hbase");
        setHoodieTableProperty(conf, "index.zookeeper_quorum", "foo, bar");
        setHoodieTableProperty(conf, "index.zookeeper_port", String.valueOf(500));
        setHoodieTableProperty(conf, "index.hbase.zknode.path", "/path/to/zk");
        setHoodieTableProperty(conf, "index.hbase_index_table", "myHbaseTable");
        Assert.assertEquals("HBASE", getHoodiePropertyFromConfig(conf, "hoodie.index.type"));

    }

    private String getHoodiePropertyFromConfig(@NonNull final Configuration conf, @NotEmpty final String property,
                                               @NonNull final Optional<String> version) {
        final HoodieIndexConfiguration indexConf = new HoodieIndexConfiguration(conf, TABLE_KEY);
        final HoodieIndexConfig hoodieIndexConfig = indexConf.configureHoodieIndex();
        final Properties props = hoodieIndexConfig.getProps();
        return props.getProperty(property);
    }

    private String getHoodiePropertyFromConfig(@NonNull final Configuration conf, @NotEmpty final String property) {
        return getHoodiePropertyFromConfig(conf, property, Optional.absent());
    }

    private void setHoodieTableProperty(@NonNull final Configuration conf, @NotEmpty final String property,
                                        @NotEmpty final String value) {
        final String fullProperty = String.format("marmaray.hoodie.tables.%s.%s", TABLE_KEY, property);
        conf.setProperty(fullProperty, value);

    }

}
