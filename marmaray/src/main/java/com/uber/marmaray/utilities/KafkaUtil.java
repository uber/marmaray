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

import com.github.rholder.retry.AttemptTimeLimiters;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.uber.marmaray.common.configuration.KafkaConfiguration;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.hibernate.validator.constraints.NotEmpty;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link KafkaUtil} provides utility methods for interacting with Kafka
 */
@Slf4j
public final class KafkaUtil {

    public static final int FETCH_OFFSET_TIMEOUT_SEC = 60;
    public static final int FETCH_OFFSET_RETRY_CNT = 3;
    // Local topic partition cache to avoid additional topic partition lookups.
    public static Map<String, List<PartitionInfo>> topicPartitions;

    private KafkaUtil() {
        throw new JobRuntimeException("This utility class should never be instantiated");
    }

    @VisibleForTesting
    public static int getFetchOffsetTimeoutSec() {
        return FETCH_OFFSET_TIMEOUT_SEC;
    }

    @VisibleForTesting
    public static int getFetchOffsetRetryCnt() {
        return FETCH_OFFSET_RETRY_CNT;
    }

    @VisibleForTesting
    public static Map<TopicPartition, Long> getTopicPartitionOffsets(@NonNull final KafkaConsumer kafkaConsumer,
        @NotEmpty final String topicName, @NonNull final Set<TopicPartition> topicPartitions) {
        final Map<TopicPartition, Long> partitionOffsets = new ConcurrentHashMap<>();
        try {
            log.info("requesting topicPartitions for {} - start", topicName);
            final AtomicInteger attemptNumber = new AtomicInteger(0);
            verifyTopicPartitions(kafkaConsumer, topicName, topicPartitions);
            final Callable<Void> fetchOffsetTask = () -> {
                log.info("requesting topicPartitions for {} - % success {}/{} - attemptNumber - {}", topicName,
                    partitionOffsets.size(), topicPartitions.size(), attemptNumber.incrementAndGet());
                topicPartitions.stream().forEach(
                    tp -> {
                        try {
                            if (!partitionOffsets.containsKey(tp)) {
                                partitionOffsets.put(tp, kafkaConsumer.position(tp));
                            }
                        } catch (Exception e) {
                            log.error("ERROR requesting topicPartitions for {} - % success {}/{}",
                                topicName, partitionOffsets.size(), topicPartitions.size(), e);
                            kafkaConsumer.wakeup();
                            throw e;
                        }
                    }
                );
                return null;
            };
            // As the kafka fetch operations can hang we would like to add timeout with retry logic while fetching
            // offsets from broker.
            final Retryer<Void> retryer = RetryerBuilder.<Void>newBuilder()
                .withAttemptTimeLimiter(AttemptTimeLimiters.fixedTimeLimit(getFetchOffsetTimeoutSec(),
                    TimeUnit.SECONDS))
                .retryIfExceptionOfType(Exception.class)
                .withStopStrategy(StopStrategies.stopAfterAttempt(getFetchOffsetRetryCnt()))
                .build();
            retryer.call(fetchOffsetTask);
            log.info("requesting topicPartitions for {} - % success {}/{} - end", topicName,
                partitionOffsets.size(), topicPartitions.size());
            Preconditions.checkState(topicPartitions.size() == partitionOffsets.size(),
                "could not retrieve offsets for few partitions");
            return partitionOffsets;
        } catch (Exception e) {
            log.error("retrieving topic partition offsets timed out for {} - % success {}/{}", topicName,
                partitionOffsets.size(), topicPartitions.size());
            throw new JobRuntimeException("failed to fetch offsets - Timeout", e);
        }
    }

    /**
     * Helper method to verify that given kafka topic has all passed in topicPartitions.
     */
    public static void verifyTopicPartitions(@NonNull final KafkaConsumer kafkaConsumer,
        @NotEmpty final String topicName, @NonNull final Set<TopicPartition> topicPartitions) {
        Set<Integer> partitions = new HashSet<>();
        topicPartitions.stream().forEach(
            tp -> {
                partitions.add(tp.partition());
            }
        );
        getTopicPartitions(kafkaConsumer, topicName).stream().forEach(p -> partitions.remove(p.partition()));
        if (!partitions.isEmpty()) {
            throw new JobRuntimeException(String.format("invalid partitions :{} : topic : {}",
                partitions.toString(), topicName));
        }
    }

    /**
     * It fetches earliest offset ranges available for given topic-partitions.
     */
    public static Map<TopicPartition, Long> getEarliestLeaderOffsets(@NonNull final KafkaConsumer kafkaConsumer,
        @NotEmpty final String topicName, @NonNull final Set<TopicPartition> topicPartitions) {
        kafkaConsumer.assign(topicPartitions);
        verifyTopicPartitions(kafkaConsumer, topicName, topicPartitions);
        final Map<TopicPartition, Long> earliestLeaderOffsets =
            kafkaConsumer.beginningOffsets(topicPartitions);
        log.info("topic-partition earliest offsets :{}", earliestLeaderOffsets);
        return earliestLeaderOffsets;
    }

    /**
     * It fetches latest offset ranges available for given topic-partitions.
     */
    public static Map<TopicPartition, Long> getLatestLeaderOffsets(@NonNull final KafkaConsumer kafkaConsumer,
        @NotEmpty final String topicName, @NonNull final Set<TopicPartition> topicPartitions) {
        kafkaConsumer.assign(topicPartitions);
        verifyTopicPartitions(kafkaConsumer, topicName, topicPartitions);
        final Map<TopicPartition, Long> latestLeaderOffsets =
            kafkaConsumer.endOffsets(topicPartitions);
        log.info("topic-partition latest offsets :{}", latestLeaderOffsets);
        return latestLeaderOffsets;
    }

    @VisibleForTesting
    public static synchronized void resetTopicPartitionCache() {
        KafkaUtil.topicPartitions = null;
    }

    /**
     * It returns available {@link TopicPartition}s for given topic.
     */
    public static Set<TopicPartition> getTopicPartitions(@NonNull final KafkaConsumer kafkaConsumer,
        @NotEmpty final String topicName) {
        try {
            Map<String, List<PartitionInfo>> topicPartitions = KafkaUtil.topicPartitions;
            if (topicPartitions == null) {
                synchronized (KafkaUtil.class) {
                    if (topicPartitions == null) {
                        topicPartitions = kafkaConsumer.listTopics();
                        KafkaUtil.topicPartitions = new ConcurrentHashMap<>(topicPartitions);
                    }
                }
            }
            if (!topicPartitions.containsKey(topicName)) {
                throw new JobRuntimeException("topic is not found :" + topicName);
            }
            final List<PartitionInfo> partitions = topicPartitions.get(topicName);
            final Set<TopicPartition> topicPartitionSet = new HashSet<>();
            partitions.forEach(
                p -> {
                    topicPartitionSet.add(new TopicPartition(p.topic(), p.partition()));
                });
            log.info("topic-partitions:{}", partitions);
            return topicPartitionSet;
        } catch (KafkaException e) {
            log.error("error retrieving topic partitions:", e);
            throw new JobRuntimeException(e);
        }
    }

    /**
     * Helper method to get {@link KafkaConsumer}.
     */
    public static KafkaConsumer getKafkaConsumer(final Map<String, String> kafkaPrams) {
        return new KafkaConsumer(kafkaPrams);
    }

    public static Map<String, Object> getKafkaParams(@NonNull final KafkaConfiguration kafkaConf) {
        final Map<String, Object> newKafkaParams = new HashMap<>();
        kafkaConf.getKafkaParams().entrySet().stream().forEach(
            entry -> {
                final String val = entry.getValue();
                try {
                    final long longVal = Long.parseLong(val);
                    if (longVal >= Integer.MAX_VALUE || longVal <= Integer.MIN_VALUE) {
                        newKafkaParams.put(entry.getKey(), longVal);
                    } else {
                        newKafkaParams.put(entry.getKey(), (int) longVal);
                    }
                    return;
                } catch (NumberFormatException e) {
                    // ignore it.
                }
                // Add all remaining (key,value) pairs as Strings.
                newKafkaParams.put(entry.getKey(), entry.getValue());
            }
        );
        return newKafkaParams;
    }
}
