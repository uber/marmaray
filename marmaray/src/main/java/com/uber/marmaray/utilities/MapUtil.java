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

import com.google.common.base.Preconditions;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.validator.constraints.NotEmpty;

/**
 * {@link MapUtil} defines utility methods for working with maps
 */
@Slf4j
public class MapUtil {

    public static final String KEY_VALUE_SEPARATOR = StringTypes.COLON;
    public static final String KEYS_SEPARATOR = StringTypes.SEMICOLON;
    public static final String ENCODING_TYPE = StandardCharsets.UTF_8.toString();

    /**
     * It returns deserialized {@link Map}. Key and values for map are string encoded using
     * {@link URLEncoder} and uses {@link #ENCODING_TYPE} for encoding.
     *
     * @param serializedMap Example format is k1{@link #KEY_VALUE_SEPARATOR}v1{@link #KEYS_SEPARATOR}k2{@link
     * #KEY_VALUE_SEPARATOR}v2.
     * @return deserialized {@link Map<String, String>}.
     */
    public static Map<String, String> deserializeMap(@NotEmpty final String serializedMap) {
        final Map<String, String> ret = new HashMap<>();
        Arrays.stream(serializedMap.trim().split(KEYS_SEPARATOR)).forEach(
            entry -> {
                final String[] keyValue = entry.split(KEY_VALUE_SEPARATOR);
                Preconditions.checkState((keyValue.length <= 2));
                if (keyValue.length == 2) {
                    try {
                        ret.put(URLDecoder.decode(keyValue[0], ENCODING_TYPE),
                            URLDecoder.decode(keyValue[1], ENCODING_TYPE));
                    } catch (UnsupportedEncodingException e) {
                        // ideally this is never going to happen.
                        log.error("Invalid encoding type :{}", ENCODING_TYPE);
                        throw new JobRuntimeException("Failed to deserialize map", e);
                    }
                }
            }
        );
        return ret;
    }

    /**
     * Helper method to serialize map.
     *
     * @param map Map to be serialized.
     */
    public static String serializeMap(@NonNull final Map<String, String> map) {
        final StringBuilder sb = new StringBuilder();
        map.entrySet().stream().forEach(
            entry -> {
                if (sb.length() > 0) {
                    sb.append(KEYS_SEPARATOR);
                }
                try {
                    sb.append(URLEncoder.encode(entry.getKey(), ENCODING_TYPE));
                    sb.append(KEY_VALUE_SEPARATOR);
                    sb.append(URLEncoder.encode(entry.getValue(), ENCODING_TYPE));
                } catch (UnsupportedEncodingException e) {
                    // ideally this is never going to happen.
                    log.error("Invalid encoding type :{}", ENCODING_TYPE);
                    throw new JobRuntimeException("Failed to serialize map", e);
                }
            }
        );
        return sb.toString();
    }
}
