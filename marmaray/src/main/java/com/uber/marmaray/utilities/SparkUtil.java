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
import com.google.common.collect.Sets;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.marmaray.common.AvroPayload;
import com.uber.marmaray.common.configuration.CassandraSinkConfiguration;
import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.configuration.ErrorTableConfiguration;
import com.uber.marmaray.common.configuration.HiveConfiguration;
import com.uber.marmaray.common.configuration.HiveSourceConfiguration;
import com.uber.marmaray.common.configuration.HoodieConfiguration;
import com.uber.marmaray.common.configuration.KafkaConfiguration;
import com.uber.marmaray.common.converters.converterresult.ConverterResult;
import com.uber.marmaray.common.converters.data.AbstractDataConverter;
import com.uber.marmaray.common.data.BinaryRawData;
import com.uber.marmaray.common.data.ErrorData;
import com.uber.marmaray.common.data.ForkData;
import com.uber.marmaray.common.data.RDDWrapper;
import com.uber.marmaray.common.data.RawData;
import com.uber.marmaray.common.data.ValidData;
import com.uber.marmaray.common.dataset.UtilRecord;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.forkoperator.ForkOperator;
import com.uber.marmaray.common.metadata.AbstractValue;
import com.uber.marmaray.common.schema.cassandra.CassandraDataField;
import com.uber.marmaray.common.schema.cassandra.CassandraSchema;
import com.uber.marmaray.common.schema.cassandra.CassandraSchemaField;
import com.uber.marmaray.common.schema.cassandra.ClusterKey;
import com.uber.marmaray.common.sinks.hoodie.HoodieSink;
import com.uber.marmaray.common.sinks.hoodie.HoodieWriteStatus;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkEnv;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.storage.RDDInfo;
import org.hibernate.validator.constraints.NotEmpty;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * {@link SparkUtil} defines utility methods for working with Apache Spark
 */
@Slf4j
public final class SparkUtil {

    public static final String SPARK_PROPERTIES_KEY_PREFIX = "spark_properties.";
    private static ThreadLocal<SerializerInstance> serializerInstance = new ThreadLocal<>();

    private SparkUtil() {
        throw new JobRuntimeException("This utility class should never be instantiated");
    }

    /**
     * @param avroSchemas avro schemas to be added to spark context for serialization
     * @param userSerializationClasses serialization classes to be added for kryo serialization
     * @param configuration config class to read and apply spark properties if present
     */
    public static SparkConf getSparkConf(@NotEmpty final String appName,
        @NonNull final Optional<List<Schema>> avroSchemas,
        @NonNull final List<Class> userSerializationClasses,
        @NonNull final Configuration configuration) {
        final SparkConf sparkConf = new SparkConf().setAppName(appName);

        /**
         * By custom registering classes the full class name of each object is not stored during serialization
         * which reduces storage space.
         *
         * Note: We don't have a way to enforce new classes which need to be serialized are added to this list.
         * We should think about adding a hook to ensure this list is current.
         */
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        final List<Class> serializableClasses = new LinkedList(Arrays.asList(
            AbstractDataConverter.class,
            AbstractValue.class,
            AvroPayload.class,
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
            scala.collection.mutable.WrappedArray.ofRef.class,
            Object[].class,
            TimestampInfo.class,
            UtilRecord.class,
            ValidData.class,
            HashMap.class,
            Optional.absent().getClass(),
            Utf8.class,
            Class.class));
        addClassesIfFound(serializableClasses,
            Arrays.asList("com.google.common.base.Present",
                "scala.reflect.ClassTag$$anon$1"));
        serializableClasses.addAll(userSerializationClasses);
        sparkConf.registerKryoClasses(serializableClasses.toArray(new Class[0]));

        if (avroSchemas.isPresent()) {
            sparkConf.registerAvroSchemas(
                    JavaConverters.iterableAsScalaIterableConverter(avroSchemas.get())
                    .asScala()
                    .toSeq());
        }

        // override spark properties
        final Map<String, String> sparkProps = configuration
            .getPropertiesWithPrefix(SPARK_PROPERTIES_KEY_PREFIX, true);
        for (Entry<String, String> entry : sparkProps.entrySet()) {
            log.info("Setting spark key:val {} : {}", entry.getKey(), entry.getValue());
            sparkConf.set(entry.getKey(), entry.getValue());
        }
        return sparkConf;
    }

    public static void addClassesIfFound(@NonNull final List<Class> serializableClasses,
        @NonNull final List<String> classList) {
        for (final String className : classList) {
            try {
                serializableClasses.add(Class.forName(className));
            } catch (ClassNotFoundException e) {
                log.error(String.format("Not adding %s to kryo serialization list", className), e);
            }
        }
    }

    public static Set<DataType> getSupportedDataTypes() {
        return Collections.unmodifiableSet(Sets.newHashSet(DataTypes.StringType, DataTypes.IntegerType,
                DataTypes.LongType, DataTypes.BooleanType, DataTypes.DoubleType,
                DataTypes.ShortType, DataTypes.FloatType));
    }

    /**
     * KryoSerializer is the the default serializaer
     * @return SerializerInstance
     */
    public static SerializerInstance getSerializerInstance() {
        if (serializerInstance.get() == null) {
            serializerInstance.set(new KryoSerializer(SparkEnv.get().conf()).newInstance());
        }
        return serializerInstance.get();
    }

    public static <T, K extends ClassTag<T>> T deserialize(@NonNull final byte[] serializedRecord,
        @NonNull final K classTag) {
        return getSerializerInstance().deserialize(ByteBuffer.wrap(serializedRecord), classTag);
    }

    public static <T, K extends ClassTag<T>> byte[] serialize(@NonNull final T record, @NonNull final K classTag) {
        return getSerializerInstance().serialize(record, classTag).array();
    }

    public static Optional<RDDInfo> getRddInfo(@NonNull final SparkContext sc, final int rddId) {
        for (RDDInfo rddInfo : sc.getRDDStorageInfo()) {
            if (rddInfo.id() != rddId) {
                continue;
            }
            return Optional.of(rddInfo);
        }
        return Optional.absent();
    }

    /**
     * All code paths should use this central method to create a {@link SparkSession} as the getOrCreate()
     * could return a previously created Spark Session with slightly different configuration (i.e hive support).
     * This can cause hard to debug failures since it isn't obvious that the SparkSession
     * returned wasn't the one that the builder attempted to create but a previously existing one.
     *
     * @return SparkSession
     */
    public static SparkSession getOrCreateSparkSession() {
        final String sparkWarning = String.join(StringTypes.SPACE,
                "Getting/Creating a Spark Session without hive support enabled."
                , "Warning: If a previous SparkSession exists it will be returned"
                , "Please check that it is using the correct configuration needed (i.e hive support, etc)"
        );
        log.info(sparkWarning);
        return SparkSession.builder().getOrCreate();
    }
}
