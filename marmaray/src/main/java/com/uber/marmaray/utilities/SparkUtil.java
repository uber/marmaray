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
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.HoodieRollbackStat;
import com.uber.hoodie.common.model.HoodieRecordLocation;
import com.uber.hoodie.common.model.HoodieWriteStat;
import com.uber.marmaray.common.HoodieErrorPayload;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.protocol.FsPermissionExtension;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkEnv;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.storage.RDDInfo;
import org.apache.spark.util.AccumulatorMetadata;
import scala.reflect.ClassManifestFactory;
import scala.reflect.ClassTag;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link SparkUtil} defines utility methods for working with Apache Spark
 */
@Slf4j
public final class SparkUtil {
    public static final ClassTag<GenericRecord> GENERIC_RECORD_CLASS_TAG =
        ClassManifestFactory.fromClass(GenericRecord.class);
    public static final ClassTag<Object> OBJECT_CLASS_TAG = ClassManifestFactory.fromClass(Object.class);

    private static ThreadLocal<SerializerInstance> serializerInstance = new ThreadLocal<>();

    private SparkUtil() {
        throw new JobRuntimeException("This utility class should never be instantiated");
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
                DataTypes.ShortType, DataTypes.FloatType, DataTypes.TimestampType, DataTypes.BinaryType));
    }

    /**
     * KryoSerializer is the the default serializaer
     *
     * @return SerializerInstance
     */
    public static SerializerInstance getSerializerInstance() {
        if (serializerInstance.get() == null) {
            serializerInstance.set(SparkEnv.get().serializer().newInstance());
        }
        return serializerInstance.get();
    }

    public static <T, K extends ClassTag<T>> T deserialize(final byte[] serializedRecord,
                                                           @NonNull final K classTag) {
        if (serializedRecord == null) {
            return null;
        }
        return getSerializerInstance().deserialize(ByteBuffer.wrap(serializedRecord), classTag);
    }

    public static <T, K extends ClassTag<T>> byte[] serialize(final T record, @NonNull final K classTag) {
        if (record == null) {
            return null;
        }
        final byte[] serializedData = getSerializerInstance().serialize(record, classTag).array();
        return serializedData;
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

    /**
     * Get list of classes we need for serialization
     *
     * @return list of classes used for serialization
     */
    public static List<Class> getSerializationClasses() {
        return new ArrayList<>(Arrays.asList(
                HoodieErrorPayload.class,
                AccumulatorMetadata.class,
                TimeUnit.class,
                HoodieRollbackStat.class,
                FileStatus.class,
                Path.class,
                FsPermissionExtension.class,
                FsAction.class,
                HoodieWriteStat.class,
                AtomicLong.class,
                HashSet.class,
                ConcurrentHashMap.class,
                WriteStatus.class,
                HoodieRecordLocation.class
        ));
    }
}
