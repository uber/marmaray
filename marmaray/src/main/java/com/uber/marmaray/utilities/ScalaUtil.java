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

import com.uber.marmaray.common.exceptions.JobRuntimeException;
import lombok.NonNull;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.immutable.Set;
import scala.collection.mutable.Buffer;

import java.util.List;
import java.util.Map;

/**
 * {@link ScalaUtil} defines scala utility methods
 */
public final class ScalaUtil {

    private ScalaUtil() {
        throw new JobRuntimeException("This utility class should never be instantiated");
    }

    public static <K, V> scala.collection.immutable.Map<K, V> toScalaMap(@NonNull final Map<K, V> javaMap) {
        return JavaConverters.mapAsScalaMapConverter(javaMap).asScala().toMap(Predef.<Tuple2<K, V>>conforms());
    }

    public static <K, V> Map<K, V> toJavaMap(@NonNull final scala.collection.Map<K, V> scalaMap) {
        return JavaConverters.mapAsJavaMapConverter(scalaMap).asJava();
    }

    public static <T> Set<T> toScalaSet(@NonNull final java.util.Set<T> javaSet) {
        return JavaConverters.asScalaSetConverter(javaSet).asScala().<T>toSet();
    }

    public static <T> java.util.Set<T> toJavaSet(@NonNull final Set<T> scalaSet) {
        return JavaConverters.setAsJavaSetConverter(scalaSet).asJava();
    }

    public static <T> List<T> toJavaList(@NonNull final Buffer<T> scalaBuffer) {
        return JavaConverters.bufferAsJavaListConverter(scalaBuffer).asJava();
    }
}
