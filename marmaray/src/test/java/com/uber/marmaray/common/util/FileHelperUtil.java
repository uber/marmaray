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

import lombok.NonNull;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.CharEncoding;
import org.hibernate.validator.constraints.NotEmpty;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

public class FileHelperUtil {
    public static <T> String getResourcePath(Class<T> clazz, String path) {
        try {
            return URLDecoder.decode(clazz.getResource(File.separator + path).getPath(), CharEncoding.UTF_8);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public static void copyResourceFileToLocalFile(@NonNull final Class clazz,
                                                   @NotEmpty final String resource,
                                                   @NonNull final File targetFile) {
        try {
            final InputStream stream = clazz.getClassLoader().getResourceAsStream(resource);
            if (stream == null) {
                throw new RuntimeException(String.format("Unable to find resource %s", resource));
            }
            FileUtils.copyInputStreamToFile(stream, targetFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
