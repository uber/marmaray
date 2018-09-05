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

import com.uber.marmaray.utilities.ConfigUtil;
import lombok.Getter;
import lombok.NonNull;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import scala.Serializable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link KafkaConfiguration} holds all the common kafka configurations.
 *
 * All common properties start with {@link #KAFKA_COMMON_PROPERTY_PREFIX}.
 */
public class KafkaConfiguration implements Serializable {

    public static final String KAFKA_COMMON_PROPERTY_PREFIX = Configuration.MARMARAY_PREFIX + "kafka.";
    public static final String KAFKA_CONNECTION_PREFIX = KAFKA_COMMON_PROPERTY_PREFIX + "conn.";
    public static final String KAFKA_BROKER_LIST = KAFKA_CONNECTION_PREFIX + "bootstrap.servers";
    public static final String KAFKA_GROUP_ID = KAFKA_CONNECTION_PREFIX + "group.id";
    public static final String KEY_DESERIALIZER = "key.deserializer";
    public static final String ENABLE_AUTO_COMMIT = "enable.auto.commit";
    public static final String VALUE_DESERIALIZER = "value.deserializer";
    public static final String GROUP_ID = "group.id";
    public static final String DEFAULT_GROUP_ID = "marmaray_group";
    public static final String ENABLE_AUTO_COMMIT_VALUE = "false";
    public static final String KAFKA_BACKOFF_MS_CONFIG = "retry.backoff.ms";
    public static final String DEFAULT_KAFKA_BACKOFF_MS_CONFIG = "20";

    @Getter
    private final Configuration conf;

    /**
     * It holds the connection related parameters required for connecting to kafka broker.
     */
    @Getter
    private final Map<String, String> kafkaParams;

    public KafkaConfiguration(@NonNull final Configuration conf) {
        this.conf = conf;
        ConfigUtil.checkMandatoryProperties(this.conf, getMandatoryProperties());
        this.kafkaParams = new HashMap<>();
        this.kafkaParams.put(GROUP_ID, DEFAULT_GROUP_ID);
        this.kafkaParams.putAll(getConf().getPropertiesWithPrefix(KAFKA_CONNECTION_PREFIX, true));
        this.kafkaParams.put(KEY_DESERIALIZER, ByteArrayDeserializer.class.getCanonicalName());
        this.kafkaParams.put(VALUE_DESERIALIZER, ByteArrayDeserializer.class.getCanonicalName());
        this.kafkaParams.put(ENABLE_AUTO_COMMIT, ENABLE_AUTO_COMMIT_VALUE);
        // If retry backoff is not set then we would want to reduce it to lower values. Default value is 400ms.
        if (!kafkaParams.containsKey(KAFKA_BACKOFF_MS_CONFIG)) {
            kafkaParams.put(KAFKA_BACKOFF_MS_CONFIG, DEFAULT_KAFKA_BACKOFF_MS_CONFIG);
        }
    }

    public List<String> getMandatoryProperties() {
        return Arrays.asList(KAFKA_BROKER_LIST);
    }
}
