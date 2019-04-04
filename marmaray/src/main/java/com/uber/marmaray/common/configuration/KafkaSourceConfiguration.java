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
import com.uber.marmaray.common.exceptions.MissingPropertyException;
import com.uber.marmaray.utilities.NumberConstants;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import java.util.concurrent.TimeUnit;
import java.util.LinkedList;
import java.util.List;

/**
 * {@link KafkaSourceConfiguration} defines configurations for Kafka source and extends {@link KafkaConfiguration}.
 *
 * All properties start with {@link #KAFKA_COMMON_PROPERTY_PREFIX}.
 */
@Slf4j
public class KafkaSourceConfiguration extends KafkaConfiguration {

    public static final String KAFKA_PROPERTY_PREFIX = KAFKA_COMMON_PROPERTY_PREFIX + "source.";
    public static final String KAFKA_TOPIC_NAME = KAFKA_PROPERTY_PREFIX + "topic_name";
    public static final String KAFKA_CLUSTER_NAME = KAFKA_PROPERTY_PREFIX + "cluster_name";
    public static final String KAFKA_MAX_MESSAGES_TO_READ = KAFKA_PROPERTY_PREFIX + "max_messages";
    public static final long DEFAULT_KAFKA_MAX_MESSAGES_TO_READ = NumberConstants.ONE_MILLION;
    public static final String KAFKA_READ_PARALLELISM = KAFKA_PROPERTY_PREFIX + "read_parallelism";
    public static final String USE_PARALLEL_BROKER_READ = KAFKA_PROPERTY_PREFIX + "use_parallel_broker_read";
    public static final boolean DEFAULT_USE_PARALLEL_BROKER_READ = false;
    /**
     * It is used for the very first run to set partition offsets for kafka topic. Expected format is "yyyy-MM-dd".
     */
    public static final String KAFKA_START_DATE = KAFKA_PROPERTY_PREFIX + "start_date";
    public static final String KAFKA_START_DATE_FORMAT = "yyyy-MM-dd";
    // epoch time in seconds
    public static final String KAFKA_START_TIME = KAFKA_PROPERTY_PREFIX + "start_time";
    public static final long MAX_KAFKA_LOOKBACK_SEC = TimeUnit.DAYS.toSeconds(7);

    @Getter
    private final String topicName;
    @Getter
    private final String clusterName;
    /**
     * start time in seconds. (inclusive).
     */
    @Getter
    private final long startTime;

    public KafkaSourceConfiguration(@NonNull final Configuration conf) {
        super(conf);
        this.topicName = getConf().getProperty(KAFKA_TOPIC_NAME).get();
        this.clusterName = getConf().getProperty(KAFKA_CLUSTER_NAME).get();

        //Try to initialize the start time. "start_date" is legacy, please use start_time moving forward.
        final Optional<String> startTimeinSeconds = getConf().getProperty(KAFKA_START_TIME);
        final Optional<String> startDate = getConf().getProperty(KAFKA_START_DATE);

        if (startTimeinSeconds.isPresent()) {
            this.startTime = Long.valueOf(startTimeinSeconds.get());
        } else if (startDate.isPresent()) {
            this.startTime = DateTime.parse(
                    startDate.get(),
                    DateTimeFormat.forPattern(KafkaSourceConfiguration.KAFKA_START_DATE_FORMAT).withZoneUTC()
            ).toDate().getTime();
        } else {
            throw new MissingPropertyException(String.format("property %s OR %s must be specified.",
                    KAFKA_START_TIME, KAFKA_START_DATE));
        }

        if (this.startTime < (TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) - MAX_KAFKA_LOOKBACK_SEC)) {
            log.error("Invalid Kafka start time/date ({})  - please specify a more recent start time.", this.startTime);
            throw new RuntimeException("Invalid kafka start time/date");
        }
    }

    public List<String> getMandatoryProperties() {
        final List<String> ret = new LinkedList<>();
        ret.addAll(super.getMandatoryProperties());
        ret.add(KAFKA_TOPIC_NAME);
        ret.add(KAFKA_CLUSTER_NAME);
        return ret;
    }

    public int getReadParallelism() {
        return Math.max(1, getConf().getIntProperty(KAFKA_READ_PARALLELISM, 1));
    }

    public boolean isParallelBrokerReadEnabled() {
        return this.getConf().getBooleanProperty(USE_PARALLEL_BROKER_READ, DEFAULT_USE_PARALLEL_BROKER_READ);
    }

    public long getMaxMessagesToRead() {
        return getConf().getLongProperty(KAFKA_MAX_MESSAGES_TO_READ, DEFAULT_KAFKA_MAX_MESSAGES_TO_READ);
    }
}
