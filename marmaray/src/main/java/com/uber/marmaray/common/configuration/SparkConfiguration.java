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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SparkConfiguration {

    public static final String SPARK_PROPERTIES_KEY_PREFIX = "spark_properties.";

    public static Map<String, String> getOverrideSparkProperties(
        @NonNull final Configuration configuration) {
        final Map<String, String> map = new HashMap<>();
        final Map<String, String> sparkProps = configuration
            .getPropertiesWithPrefix(SPARK_PROPERTIES_KEY_PREFIX, true);
        for (Entry<String, String> entry : sparkProps.entrySet()) {
            log.info("Setting spark key:val {} : {}", entry.getKey(), entry.getValue());
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }

    public static void overrideSparkConfInConfiguration(
        @NonNull final Configuration configuration,
        @NonNull final Map<String, String> sparkConfigOverrides) {
        for (Entry<String, String> me : sparkConfigOverrides.entrySet()) {
            final String sparkConfigKey = me.getKey();
            final String sparkConfigValue = me.getValue();
            log.info("Overriding spark key:val {} : {}", sparkConfigKey, sparkConfigValue);
            configuration.setProperty(SPARK_PROPERTIES_KEY_PREFIX + sparkConfigKey, sparkConfigValue);
        }
    }
}
