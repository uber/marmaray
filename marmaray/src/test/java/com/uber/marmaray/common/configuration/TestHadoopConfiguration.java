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

import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class TestHadoopConfiguration {

    @Test
    public void testHadoopConf() {
        final Configuration conf = new Configuration();
        final Map<String, String> hadoopProps = new HashMap<>();
        hadoopProps.put("hadoopProp1", "value1");
        hadoopProps.put("hadoopProp2", "value2");
        hadoopProps.entrySet().stream().forEach(entry -> conf
            .setProperty(HadoopConfiguration.HADOOP_COMMON_PROPERTY_PREFIX + entry.getKey(), entry.getValue()));
        final Map<String, String> nonHadoopProps = new HashMap<>();
        nonHadoopProps.put("nonHadoopProp1", "value1");
        nonHadoopProps.put("nonHadoopProp2", "value2");
        nonHadoopProps.entrySet().stream().forEach(entry -> conf.setProperty(entry.getKey(), entry.getValue()));
        final org.apache.hadoop.conf.Configuration hadoopConf = new HadoopConfiguration(conf).getHadoopConf();
        Assert.assertTrue(hadoopProps.entrySet().stream()
            .allMatch(entry -> hadoopConf.get(entry.getKey()).equals(entry.getValue())));
        Assert.assertTrue(nonHadoopProps.entrySet().stream()
            .noneMatch(entry -> hadoopConf.get(entry.getKey()) != null));
    }
}
