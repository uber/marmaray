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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.factories.ReflectionSerializerFactory;
import com.esotericsoftware.kryo.factories.SerializerFactory;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.google.common.base.Optional;
import lombok.NonNull;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;

/**
 * {@link MarmarayKryoSerializer} is a helper class for registering custom serializer instead of
 * using KryoSerializer where KryoSerializer is unable to serialize certain objects.
 * To use this while running spark job; set "spark.serializer" spark config with
 * {@link MarmarayKryoSerializer}'s absolute class name.
 */
public class MarmarayKryoSerializer extends KryoSerializer {

    public MarmarayKryoSerializer(@NonNull final SparkConf sparkConf) {
        super(sparkConf);
    }

    @Override
    public Kryo newKryo() {
        final Kryo kryo = super.newKryo();
        kryo.setDefaultSerializer(
                new SerializerFactory() {
                    private final SerializerFactory defaultSerializer =
                            new ReflectionSerializerFactory(FieldSerializer.class);
                    @Override
                    public Serializer makeSerializer(@NonNull final Kryo kryo, @NonNull final Class<?> type) {
                        final Optional<Serializer> serializer = MarmarayKryoSerializer.this.getSerializer(type);
                        if (serializer.isPresent()) {
                            return serializer.get();
                        }
                        return this.defaultSerializer.makeSerializer(kryo, type);
                    }
                }
        );
        return kryo;
    }

    /**
     * @param type classType for which we need a serializer
     * @return It returns custom serializer for given class otherwise will return {@link Optional#absent()}
     */
    protected Optional<Serializer> getSerializer(@NonNull final Class<?> type) {
        return Throwable.class.isAssignableFrom(type) ? Optional.of(new JavaSerializer()) : Optional.absent();
    }
}
