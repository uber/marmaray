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

import com.uber.marmaray.utilities.NumberConstants;
import lombok.Getter;
import lombok.NonNull;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import java.util.LinkedList;
import java.util.List;

/**
 * {@link KafkaSourceConfiguration} defines configurations for Kafka source and extends {@link KafkaConfiguration}.
 *
 * All properties start with {@link #KAFKA_COMMON_PROPERTY_PREFIX}.
 */
public class KafkaSourceConfiguration extends KafkaConfiguration {

    public static final String KAFKA_PROPERTY_PREFIX = KAFKA_COMMON_PROPERTY_PREFIX + "source.";
    public static final String KAFKA_TOPIC_NAME = KAFKA_PROPERTY_PREFIX + "topic_name";
    public static final String KAFKA_CLUSTER_NAME = KAFKA_PROPERTY_PREFIX + "cluster_name";
    public static final String KAFKA_MAX_MESSAGES_TO_READ = KAFKA_PROPERTY_PREFIX + "max_messages";
    public static final long DEFAULT_KAFKA_MAX_MESSAGES_TO_READ = NumberConstants.ONE_MILLION;
    public static final String KAFKA_READ_PARALLELISM = KAFKA_PROPERTY_PREFIX + "read_parallelism";
    /**
     * It is used for the very first run to set partition offsets for kafka topic. Expected format is "yyyy-MM-dd".
     */
    public static final String KAFKA_START_DATE = KAFKA_PROPERTY_PREFIX + "start_date";
    public static final String KAFKA_START_DATE_FORMAT = "yyyy-MM-dd";

    @Getter
    private final String topicName;
    @Getter
    private final String clusterName;
    /**
     * start time in millis. (inclusive).
     */
    @Getter
    private final long startTime;

    public KafkaSourceConfiguration(@NonNull final Configuration conf) {
        super(conf);
        this.topicName = getConf().getProperty(KAFKA_TOPIC_NAME).get();
        this.clusterName = getConf().getProperty(KAFKA_CLUSTER_NAME).get();
        this.startTime =
            DateTime.parse(getConf().getProperty(KAFKA_START_DATE).get(),
                DateTimeFormat.forPattern(KafkaSourceConfiguration.KAFKA_START_DATE_FORMAT).withZoneUTC()
            ).toDate().getTime();
    }

    public List<String> getMandatoryProperties() {
        final List<String> ret = new LinkedList<>();
        ret.addAll(super.getMandatoryProperties());
        ret.add(KAFKA_TOPIC_NAME);
        ret.add(KAFKA_CLUSTER_NAME);
        ret.add(KAFKA_START_DATE);
        return ret;
    }

    public int getReadParallelism() {
        return Math.max(1, getConf().getIntProperty(KAFKA_READ_PARALLELISM, 1));
    }

    public long getMaxMessagesToRead() {
        return getConf().getLongProperty(KAFKA_MAX_MESSAGES_TO_READ, DEFAULT_KAFKA_MAX_MESSAGES_TO_READ);
    }
}
