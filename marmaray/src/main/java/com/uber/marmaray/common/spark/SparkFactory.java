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

package com.uber.marmaray.common.spark;

import com.google.common.base.Optional;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.marmaray.common.AvroPayload;
import com.uber.marmaray.common.configuration.CassandraSinkConfiguration;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.configuration.ErrorTableConfiguration;
import com.uber.marmaray.common.configuration.HadoopConfiguration;
import com.uber.marmaray.common.configuration.HiveConfiguration;
import com.uber.marmaray.common.configuration.HiveSourceConfiguration;
import com.uber.marmaray.common.configuration.HoodieConfiguration;
import com.uber.marmaray.common.configuration.KafkaConfiguration;
import com.uber.marmaray.common.configuration.SparkConfiguration;
import com.uber.marmaray.common.converters.converterresult.ConverterResult;
import com.uber.marmaray.common.converters.data.AbstractDataConverter;
import com.uber.marmaray.common.data.BinaryRawData;
import com.uber.marmaray.common.data.ErrorData;
import com.uber.marmaray.common.data.ForkData;
import com.uber.marmaray.common.data.RDDWrapper;
import com.uber.marmaray.common.data.RawData;
import com.uber.marmaray.common.data.ValidData;
import com.uber.marmaray.common.dataset.UtilRecord;
import com.uber.marmaray.common.forkoperator.ForkOperator;
import com.uber.marmaray.common.metadata.AbstractValue;
import com.uber.marmaray.common.schema.cassandra.CassandraDataField;
import com.uber.marmaray.common.schema.cassandra.CassandraSchema;
import com.uber.marmaray.common.schema.cassandra.CassandraSchemaField;
import com.uber.marmaray.common.schema.cassandra.ClusterKey;
import com.uber.marmaray.common.sinks.hoodie.HoodieSink;
import com.uber.marmaray.common.sinks.hoodie.HoodieWriteStatus;
import com.uber.marmaray.utilities.SparkUtil;
import com.uber.marmaray.utilities.TimestampInfo;
import com.uber.marmaray.utilities.listener.SparkEventListener;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.util.Utf8;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;
import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray.ofRef;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * {@link SparkFactory} is responsible for creating any Spark related resource such as
 * {@link JavaSparkContext} or {@link SparkSession}, or even {@link SparkConf} which is required
 * to create the former two. Pass this object around in code to access the above mentioned resources.
 */
@Slf4j
@RequiredArgsConstructor
public class SparkFactory {

    @NonNull
    @Getter
    private final SparkArgs sparkArgs;
    private Optional<SparkSession> sparkSessionOptional = Optional.absent();
    /**
     * Uses {@link SparkSession} returned from {@link SparkFactory#getSparkSession}
     * to create {@link JavaSparkContext}. See {@link SparkFactory#getSparkSession}
     * for {@link SparkSession} that is retrieved.
     */
    public synchronized JavaSparkContext getSparkContext() {
        return new JavaSparkContext(getSparkSession().sparkContext());
    }

    /**
     * Uses existing {@link SparkSession} if present, else creates a new one
     */
    public synchronized SparkSession getSparkSession() {
        if (this.sparkSessionOptional.isPresent()) {
            return this.sparkSessionOptional.get();
        }
        final Builder sparkSessionBuilder = SparkSession.builder();
        if (this.sparkArgs.isHiveSupportEnabled()) {
            sparkSessionBuilder.enableHiveSupport();
        }
        this.sparkSessionOptional = Optional.of(sparkSessionBuilder
            .config(createSparkConf()).getOrCreate());
        log.info("Created new SparkSession using {}", sparkArgs);
        updateSparkContext(sparkArgs, this.sparkSessionOptional.get().sparkContext());
        return this.sparkSessionOptional.get();
    }

    /**
     * Creates {@link SparkConf} with {@link org.apache.spark.serializer.KryoSerializer} along with
     * registering default/user-input serializable classes and user-input Avro Schemas.
     * Once {@link SparkContext} is created, we can no longer register serialization classes and Avro schemas.
     */
    public SparkConf createSparkConf() {
        /**
         * By custom registering classes the full class name of each object
         * is not stored during serialization which reduces storage space.
         */
        final SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.serializer", MarmarayKryoSerializer.class.getName());
        // We don't want to cache too many connections on kafka consumer so we will be limiting it to 4 per executor
        // default value is 64. See org.apache.spark.streaming.kafka010.KafkaRDD for more information
        sparkConf.set("spark.streaming.kafka.consumer.cache.initialCapacity", "4");
        sparkConf.set("spark.streaming.kafka.consumer.cache.maxCapacity", "4");

        final List<Class> serializableClasses = getDefaultSerializableClasses();
        serializableClasses.addAll(this.sparkArgs.getUserSerializationClasses());
        sparkConf.registerKryoClasses(serializableClasses.toArray(new Class[0]));

        sparkConf.registerAvroSchemas(
            JavaConverters
                .iterableAsScalaIterableConverter(this.sparkArgs.getAvroSchemas())
                .asScala()
                .toSeq());

        // override spark properties
        final Map<String, String> sparkProps = SparkConfiguration
            .getOverrideSparkProperties(this.sparkArgs.getConfiguration());
        for (Entry<String, String> entry : sparkProps.entrySet()) {
            log.info("Setting spark key:val {} : {}", entry.getKey(), entry.getValue());
            sparkConf.set(entry.getKey(), entry.getValue());
        }
        return sparkConf;
    }

    /**
     * Stops any existing {@link SparkSession} and removes reference to it
     */
    public synchronized void stop() {
        if (this.sparkSessionOptional.isPresent()) {
            this.sparkSessionOptional.get().stop();
            this.sparkSessionOptional = Optional.absent();
        }
    }

    /**
     * Hook for plugging in custom SparkListers
     */
    protected List<SparkListener> getSparkEventListeners() {
        return Arrays.asList(new SparkEventListener());
    }

    private void updateSparkContext(@NonNull final SparkArgs sparkArgs,
        @NonNull final SparkContext sc) {
        for (SparkListener sparkListener : getSparkEventListeners()) {
            sc.addSparkListener(sparkListener);
        }
        sc.hadoopConfiguration().addResource(
            new HadoopConfiguration(sparkArgs.getConfiguration()).getHadoopConf());
    }

    private List<Class> getDefaultSerializableClasses() {
        final List<Class> serializableClasses = new LinkedList(Arrays.<Class>asList(
            AbstractDataConverter.class,
            AbstractValue.class,
            ArrayList.class,
            BinaryRawData.class,
            CassandraDataField.class,
            CassandraSchema.class,
            CassandraSchemaField.class,
            CassandraSinkConfiguration.class,
            ClusterKey.class,
            Configuration.class,
            ConverterResult.class,
            ErrorTableConfiguration.class,
            ErrorData.class,
            ForkData.class,
            ForkOperator.class,
            HiveConfiguration.class,
            HiveSourceConfiguration.class,
            HoodieConfiguration.class,
            HoodieSink.class,
            HoodieRecord.class,
            HoodieKey.class,
            HoodieWriteStatus.class,
            KafkaConfiguration.class,
            java.util.Optional.class,
            Optional.class,
            RawData.class,
            RDDWrapper.class,
            ofRef.class,
            Object[].class,
            TimestampInfo.class,
            UtilRecord.class,
            ValidData.class,
            HashMap.class,
            Optional.absent().getClass(),
            Utf8.class,
            Class.class));
        serializableClasses.addAll(AvroPayload.getSerializationClasses());

        SparkUtil.addClassesIfFound(serializableClasses,
            Arrays.asList(
                "com.google.common.base.Present",
                "scala.reflect.ClassTag$$anon$1"));
        return serializableClasses;
    }
}
