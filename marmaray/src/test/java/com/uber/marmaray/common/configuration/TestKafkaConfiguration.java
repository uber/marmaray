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

import com.uber.marmaray.common.util.KafkaTestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static com.uber.marmaray.common.configuration.KafkaConfiguration.DEFAULT_GROUP_ID;
import static com.uber.marmaray.common.configuration.KafkaConfiguration.ENABLE_AUTO_COMMIT;
import static com.uber.marmaray.common.configuration.KafkaConfiguration.ENABLE_AUTO_COMMIT_VALUE;
import static com.uber.marmaray.common.configuration.KafkaConfiguration.GROUP_ID;
import static com.uber.marmaray.common.configuration.KafkaConfiguration.KAFKA_BROKER_LIST;
import static com.uber.marmaray.common.configuration.KafkaConfiguration.KAFKA_GROUP_ID;
import static com.uber.marmaray.common.configuration.KafkaSourceConfiguration.KAFKA_PROPERTY_PREFIX;

public class TestKafkaConfiguration {

    public static final String BROKER_LIST = "foo";

    @Test
    public void testAutoCommitNotConfigurable() {
        final Configuration conf = new Configuration();
        KafkaTestHelper.setMandatoryConf(conf,
                Arrays.asList(KAFKA_BROKER_LIST, KAFKA_PROPERTY_PREFIX + ENABLE_AUTO_COMMIT),
                Arrays.asList(BROKER_LIST, "bar"));
        final KafkaConfiguration kafkaConf = new KafkaConfiguration(conf);
        Assert.assertEquals(ENABLE_AUTO_COMMIT_VALUE, kafkaConf.getKafkaParams().get(ENABLE_AUTO_COMMIT));
    }

    @Test
    public void testGroupIdConfigurable() {
        final String myGroupId = "mygroup";
        final Configuration conf = new Configuration();
        KafkaTestHelper.setMandatoryConf(conf, Arrays.asList(KAFKA_BROKER_LIST, KAFKA_GROUP_ID),
                Arrays.asList(BROKER_LIST, myGroupId));
        final KafkaConfiguration kafkaConf = new KafkaConfiguration(conf);
        Assert.assertEquals(myGroupId, kafkaConf.getKafkaParams().get(GROUP_ID));
    }

    @Test
    public void testGroupIdDefault() {
        final Configuration conf = new Configuration();
        KafkaTestHelper.setMandatoryConf(conf, Collections.singletonList(KAFKA_BROKER_LIST),
                Collections.singletonList(BROKER_LIST));
        final KafkaConfiguration kafkaConf = new KafkaConfiguration(conf);
        Assert.assertEquals(DEFAULT_GROUP_ID, kafkaConf.getKafkaParams().get(GROUP_ID));
    }

}