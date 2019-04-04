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
package com.uber.marmaray.common.util;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.configuration.KafkaSourceConfiguration;
import com.uber.marmaray.common.exceptions.InvalidDataException;
import com.uber.marmaray.common.schema.ISchemaService;
import com.uber.marmaray.utilities.GenericRecordUtil;
import com.uber.marmaray.utilities.ScalaUtil;
import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.serializer.DefaultEncoder;
import kafka.serializer.StringEncoder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.spark.streaming.kafka010.KafkaTestUtils;
import org.hibernate.validator.constraints.NotEmpty;

import java.io.FileNotFoundException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static com.uber.marmaray.common.configuration.KafkaConfiguration.KAFKA_BROKER_LIST;
import static com.uber.marmaray.common.configuration.KafkaSourceConfiguration.KAFKA_CLUSTER_NAME;
import static com.uber.marmaray.common.configuration.KafkaSourceConfiguration.KAFKA_START_DATE;
import static com.uber.marmaray.common.configuration.KafkaSourceConfiguration.KAFKA_START_TIME;
import static com.uber.marmaray.common.configuration.KafkaSourceConfiguration.KAFKA_TOPIC_NAME;

@Slf4j
public class KafkaTestHelper {

    public static final String SCHEMA_SERVICE_TEST_LOCAL_PATH = "src/test/resources/schema-service";
    public static final String TEST_KAFKA_CLUSTER_NAME = "test-cluster";
    public static final String TEST_KAFKA_START_DATE =
            new SimpleDateFormat(KafkaSourceConfiguration.KAFKA_START_DATE_FORMAT).format(new Date());
    private static final String STRING_VALUE_DEFAULT = "value";
    private static final Boolean BOOLEAN_VALUE_DEFAULT = true;

    public static void createTopicPartitions(@NonNull final KafkaTestUtils kafkaTestUtils, final String topicName,
        final int partitions) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(topicName));
        // Create topic.
        kafkaTestUtils.createTopic(topicName, partitions);
    }

    public static ProducerConfig getProducerConfig(@NonNull final KafkaTestUtils kafkaTestUtils) {
        final Properties props = new Properties();
        props.put("metadata.broker.list", kafkaTestUtils.brokerAddress());
        props.put("serializer.class", DefaultEncoder.class.getName());
        props.put("key.serializer.class", StringEncoder.class.getName());
        // wait for all in-sync replicas to ack sends
        props.put("request.required.acks", "-1");
        return new ProducerConfig(props);
    }

    /**
     * It publishes kafka messages to specified partitions[index is used as partition number].
     */
    public static void publishMessages(@NonNull final KafkaTestUtils kafkaTestUtils, final String topicName,
        @NonNull final List<List<byte[]>> messagesList) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(topicName));
        final Producer<String, byte[]> producer = new Producer<>(getProducerConfig(kafkaTestUtils));
        final Set<KeyedMessage<String, byte[]>> messageSet = new HashSet<>();
        for (int partition = 0; partition < messagesList.size(); partition++) {
            final String key = Integer.toString(partition);
            messagesList.get(partition).stream().forEach(
                message -> {
                    messageSet.add(new KeyedMessage<>(topicName, key, message));
                }
            );
        }

        producer.send(ScalaUtil.toScalaSet(messageSet).toSeq());
        producer.close();
    }

    public static List<List<byte[]>> generateMessages(@NonNull final List<Integer> messageCountList,
                                                      @NonNull final Schema schema,
                                                      @NotEmpty final Integer schemaVersion) {
        final TestKafkaSchemaService schemaService = new TestKafkaSchemaService();
        final ISchemaService.ISchemaServiceWriter writer =
            schemaService.getWriter(schema.getName(), schemaVersion);
        return writeMessages(messageCountList, schema, writer);
    }

    public static List<List<byte[]>> writeMessages(@NonNull final List<Integer> messageCountList,
                           @NonNull final Schema schema,
                           @NonNull final ISchemaService.ISchemaServiceWriter writer) {
        final List<List<byte[]>> ret = new ArrayList<>(messageCountList.size());
        messageCountList.stream().forEach(
                messageCount -> {
                    ret.add(KafkaTestHelper.getTestData(schema, messageCount).stream().map(
                            record -> {
                                try {
                                    return writer.write(record);
                                } catch (InvalidDataException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                    ).collect(Collectors.toList()));
                }
        );
        return ret;
    }

    public static Schema getSchema(@NotEmpty final String schemaName) {
        return SchemaBuilder.record(schemaName).fields()
                .name("testBoolean").type().optional().booleanType()
                .name("testLong").type().optional().longType()
                .name("testString").type().optional().stringType()
                .endRecord();
    }

    public static List<GenericRecord> getTestData(@NonNull final Schema schema, final int numRecords) {
        final List<GenericRecord> records = new ArrayList<>(numRecords);
        for (int i = 0; i < numRecords; i++) {
            final GenericRecord record = new GenericData.Record(schema);
            schema.getFields().forEach(field -> record.put(field.name(), getDefaultValue(field)));
            records.add(record);
        }
        return records;
    }

    private static Object getDefaultValue(@NonNull final Schema.Field field) {
        if (GenericRecordUtil.isOptional(field.schema())) {
            final Iterator<Schema> iter = field.schema().getTypes().iterator();
            while (iter.hasNext()) {
                final Schema next = iter.next();
                if (next.getType() != Schema.Type.NULL) {
                    return getDefaultValue(next.getType());
                }
            }
            // no non-null schema type found, just return null
            return null;
        } else {
            return getDefaultValue(field.schema().getType());
        }
    }

    private static Object getDefaultValue(@NonNull final Schema.Type type) {
        if (type == Schema.Type.STRING) {
            return STRING_VALUE_DEFAULT;
        } else if (type == Schema.Type.BOOLEAN) {
            return BOOLEAN_VALUE_DEFAULT;
        } else if (type == Schema.Type.LONG) {
            return System.currentTimeMillis();
        } else if (type == Schema.Type.UNION) {
            return null;
        } else {
            log.warn("Found unknown type {}, returning null", type.toString());
            return null;
        }
    }

    public static void publishMessagesToKafkaTopics(@NonNull final KafkaTestUtils kafkaTestUtils,
        @NotEmpty final String topicName)
        throws FileNotFoundException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(topicName));
        // Generate and publish messages to kafka.
        final List<Integer> partitionNumMessages = Arrays.asList(3, 5, 6, 7, 10, 25, 35, 45);
        final List<List<byte[]>> messages = KafkaTestHelper.generateMessages(partitionNumMessages,
                getSchema(topicName), 1);
        KafkaTestHelper.publishMessages(kafkaTestUtils, topicName, messages);
    }

    public static KafkaSourceConfiguration getKafkaSourceConfiguration(@NotEmpty final String topicName,
                                                                       @NotEmpty final String brokerAddress,
                                                                       final String startDate,
                                                                       final String startTime) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(topicName));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(brokerAddress));
        final Configuration conf = new Configuration();
        KafkaTestHelper.setMandatoryConf(conf,
                Arrays.asList(
                        KAFKA_BROKER_LIST, KAFKA_TOPIC_NAME, KAFKA_CLUSTER_NAME, KAFKA_START_DATE, KAFKA_START_TIME),
                Arrays.asList(brokerAddress, topicName, TEST_KAFKA_CLUSTER_NAME, startDate, startTime));
        return new KafkaSourceConfiguration(conf);
    }

    public static KafkaSourceConfiguration getKafkaSourceConfiguration(@NotEmpty final String topicName,
        @NotEmpty final String brokerAddress, @NotEmpty final String startDate) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(topicName));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(brokerAddress));
        final Configuration conf = new Configuration();
        KafkaTestHelper.setMandatoryConf(conf, Arrays.asList(KAFKA_BROKER_LIST, KAFKA_TOPIC_NAME, KAFKA_CLUSTER_NAME,
            KAFKA_START_DATE), Arrays.asList(brokerAddress, topicName, TEST_KAFKA_CLUSTER_NAME, startDate));
        return new KafkaSourceConfiguration(conf);
    }

    public static KafkaSourceConfiguration getKafkaSourceConfiguration(final String topicName,
        final String brokerAddress) {
        return getKafkaSourceConfiguration(topicName, brokerAddress, TEST_KAFKA_START_DATE);
    }

    public static void setMandatoryConf(final Configuration conf, final List<String> mandatoryProperties,
        final List<String> values) {
        for (int i = 0; i < mandatoryProperties.size(); i++) {
            conf.setProperty(mandatoryProperties.get(i), values.get(i));
        }
    }

    public static class TestKafkaSchemaService implements ISchemaService, Serializable {


        @Override public Schema getWrappedSchema(final String schemaName) {
            return KafkaTestHelper.getSchema(schemaName);
        }

        @Override public Schema getSchema(final String schemaName) {
            return getWrappedSchema(schemaName);
        }

        @Override public ISchemaServiceWriter getWriter(final String schemaName, final int schemaVersion) {
            return new TestKafkaSchemaServiceWriter();
        }

        @Override public ISchemaServiceReader getReader(final String schemaName, final int schemaVersion) {
            return new TestKafkaSchemaServiceReader();
        }

        private class TestKafkaSchemaServiceWriter implements ISchemaServiceWriter, Serializable {
            @Override public byte[] write(final GenericRecord record) throws InvalidDataException {
                return new byte[0];
            }
        }

        private class TestKafkaSchemaServiceReader implements ISchemaServiceReader, Serializable {
            @Override public GenericRecord read(final byte[] buffer) throws InvalidDataException {
                return new GenericRecordBuilder(getSchema("testTopic")).build();
            }
        }
    }
}
