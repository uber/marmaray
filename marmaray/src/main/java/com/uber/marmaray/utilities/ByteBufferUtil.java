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

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * {@link ByteBufferUtil }Provides standard ByteBuffer functionality to convert types to and from ByteBuffers
 */
public final class ByteBufferUtil {
    private ByteBufferUtil() {
        throw new JobRuntimeException("This is a utility class that should not be instantiated");
    }

    public static String convertToString(final ByteBuffer bb) {
        return new String(bb.array(), Charset.forName(StandardCharsets.UTF_8.toString()));
    }

    public static ByteBuffer wrap(final String value) {
        try {
            return ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8.toString()));
        } catch (final UnsupportedEncodingException e) {
            // should never see this
            throw new JobRuntimeException(
                    String.format("Unsupported encoding exception on string: %s. Error Message: %s",
                            value, e.getMessage()));
        }
    }
}
