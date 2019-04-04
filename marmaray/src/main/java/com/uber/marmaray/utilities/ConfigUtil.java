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

import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import com.uber.marmaray.common.exceptions.MissingPropertyException;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * {@link ConfigUtil} provides utility methods for job configurations
 */
@Slf4j
public final class ConfigUtil {

    private ConfigUtil() {
        throw new JobRuntimeException("This utility class should never be instantiated");
    }

    /**
     * Checks if all mandatory properties are present or not. If not Present then it will throw
     * {@link MissingPropertyException}
     */
    public static void checkMandatoryProperties(@NonNull final Configuration conf,
                                                @NonNull final List<String> mandatoryProps) {
        if (mandatoryProps.isEmpty()) {
            log.warn("mandatory properties are empty");
        }
        mandatoryProps.stream().forEach(
            prop -> {
                if (!conf.getProperty(prop).isPresent()) {
                    log.error("Missing property:{} existing conf:{}", prop, conf);
                    throw new MissingPropertyException("property:" + prop);
                }
            });
    }
}
