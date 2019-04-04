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

package com.uber.marmaray.utilities;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import jodd.io.StringInputStream;
import org.apache.commons.collections.map.SingletonMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Matchers.anyLong;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({KafkaUtil.class})
public class TestKafkaUtil {

    @Test
    public void testRequestTimeout() {
        mockStatic(KafkaUtil.class);
        final String topicName = "testtopic";
        final Set<TopicPartition> topicPartitions = new HashSet<TopicPartition>();
        final List<PartitionInfo> partitionInfos = new LinkedList<>();
        final int numPartitions = 5;
        final int fetchTimeoutSec = 1;
        final int fetchRetryCnt = 2;
        when(KafkaUtil.getFetchOffsetRetryCnt()).thenReturn(fetchRetryCnt);
        when(KafkaUtil.getFetchOffsetTimeoutSec()).thenReturn(fetchTimeoutSec);


        IntStream.range(0, numPartitions).forEach(
            i -> {
                topicPartitions.add(new TopicPartition(topicName, i));
                partitionInfos.add(new PartitionInfo(topicName, i, null, null, null));
            }
        );
        final Map<Integer, AtomicInteger> attempts = new HashMap();
        final KafkaConsumer kafkaConsumer = mock(KafkaConsumer.class);
        when(kafkaConsumer.partitionsFor(Matchers.anyString())).thenReturn(partitionInfos);
        when(KafkaUtil.getTopicPartitionOffsets(Matchers.any(KafkaConsumer.class),
            Matchers.anyString(), Matchers.any(Set.class))).thenCallRealMethod();
        when(kafkaConsumer.position(Matchers.any(TopicPartition.class))).thenAnswer(
            (Answer<Long>) invocationOnMock -> {
                final TopicPartition tp = invocationOnMock.getArgumentAt(0, TopicPartition.class);
                if (!attempts.containsKey(tp.partition())) {
                    attempts.put(tp.partition(), new AtomicInteger(1));
                } else {
                    attempts.get(tp.partition()).incrementAndGet();
                }
                if (tp.partition() == numPartitions - 1) {
                    // ensure timeout occurs by adding 10 additional ms to max.
                    Thread.sleep(TimeUnit.SECONDS.toMillis(fetchTimeoutSec) + 10);
                }
                return tp.partition() * 2L;
            }
        );
        try {
            KafkaUtil.getTopicPartitionOffsets(kafkaConsumer, topicName, topicPartitions);
            Assert.fail("exception is expected");
        } catch (JobRuntimeException e) {
            // ignore it.
        }
        Assert.assertEquals(fetchRetryCnt, attempts.get(numPartitions-1).get());
        IntStream.range(0, numPartitions - 1).forEach(
            i -> {
                Assert.assertTrue(!attempts.containsKey(i) || attempts.get(i).get() < 2);
            }
        );
    }


}
