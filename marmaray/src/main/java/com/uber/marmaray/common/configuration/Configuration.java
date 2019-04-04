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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.hibernate.validator.constraints.NotEmpty;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * {@link Configuration} will be instantiated from a YAML based file
 * and contain all the pertinent metadata to initialize and execute
 * a data transfer job. Supports scopes and scope based config
 * overriding. Refer to documentation of {@link ConfigScopeResolver}
 * for more info on scope overriding
 */
@Slf4j
public class Configuration implements Serializable {

    public static final String MARMARAY_PREFIX = "marmaray.";
    public static final String SCOPE_OVERRIDE_MAPPING_KEY = "scope_override_map";

    private final Properties props = new Properties();

    /**
     * @deprecated todo: remove this constructor in a separate diff
     * since callers will need to inject scope, so will need change in callers
     */
    @Deprecated
    public Configuration() {

    }

    public Configuration(@NonNull final File yamlFile,
        @NonNull final Optional<String> scope) {
        loadYamlFile(yamlFile, scope);
    }

    public Configuration(@NonNull final InputStream inputStream,
        @NonNull final Optional<String> scope) {
        loadYamlStream(inputStream, scope);
    }

    /**
     * @deprecated todo: remove this constructor in a separate diff
     * since callers will need to inject scope, so will need change in callers
     */
    @Deprecated
    public Configuration(@NonNull final Configuration conf) {
        this.props.putAll(conf.props);
    }

    @VisibleForTesting
    public Configuration(@NonNull final Properties properties) {
        this.props.putAll(properties);
    }

    private void loadYamlFile(@NonNull final File yamlFile,
        final Optional<String> scope) {
        try {
            final FileSystem localFs = FileSystem.getLocal(
                new HadoopConfiguration(new Configuration()).getHadoopConf());
            final InputStream yamlInputStream = localFs.open(new Path(yamlFile.getPath()));
            loadYamlStream(yamlInputStream, scope);
        } catch (IOException e) {
            final String errorMsg = String
                .format("Error loading yaml config file %s", yamlFile.getAbsolutePath());
            log.error(errorMsg, e);
            throw new JobRuntimeException(errorMsg, e);
        }
    }

    public void loadYamlStream(@NonNull final InputStream yamlStream,
        @NonNull final Optional<String> scope) {
        try {
            final ObjectMapper yamlReader = new ObjectMapper(new YAMLFactory());
            final JsonNode jsonNode = yamlReader.readTree(yamlStream);
            final JsonNode scopeOverriddenJsonNode = handleScopeOverriding(scope, jsonNode);
            parseConfigJson(scopeOverriddenJsonNode, "");
        } catch (IOException e) {
            final String errorMsg = "Error loading config from stream";
            log.error(errorMsg, e);
            throw new JobRuntimeException(errorMsg, e);
        }
    }

    private JsonNode handleScopeOverriding(
        @NonNull final Optional<String> scope, @NonNull final JsonNode jsonNode) {
        return new ConfigScopeResolver(SCOPE_OVERRIDE_MAPPING_KEY)
            .projectOverrideScopeOverDefault(scope, jsonNode);
    }

    public String getProperty(final String key, final String defaultValue) {
        return this.props.getProperty(key, defaultValue);
    }

    public Optional<String> getProperty(final String key) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(key));
        final String val = this.props.getProperty(key);
        return (val == null) ? Optional.absent() : Optional.of(val);
    }

    public void setProperty(final String key, final String value) {
        this.props.setProperty(key, value);
    }

    /**
     * Returns properties with or without prefix.
     * @param prefix
     * @param removePrefix if true it will remove prefix from properties.
     * @return {@link Map} with properties.
     */
    public Map<String, String> getPropertiesWithPrefix(final String prefix, final boolean removePrefix) {
        Preconditions.checkState(!Strings.isNullOrEmpty(prefix));
        final Map<String, String> properties = new HashMap<>();
        final int prefixLength = prefix.length();
        this.props.entrySet().forEach(
            entry -> {
                final String key = (String) entry.getKey();
                if (key.startsWith(prefix)) {
                    if (removePrefix) {
                        properties.put(key.substring(prefixLength), entry.getValue().toString());
                    } else {
                        properties.put(key, entry.getValue().toString());
                    }

                }
            });
        return properties;
    }

    public Properties getProperties() {
        final Properties properties = new Properties();
        this.props.forEach((k, v) -> properties.put(k, v));
        return properties;
    }

    public static <T> T getProperty(@NonNull final Configuration conf, @NotEmpty final String key,
        @NonNull final T defaultValue) {
        if (defaultValue instanceof Integer) {
            return (T) new Integer(conf.getIntProperty(key, ((Integer) defaultValue).intValue()));
        } else if (defaultValue instanceof Long) {
            return (T) new Long(conf.getLongProperty(key, ((Long) defaultValue).longValue()));
        } else if (defaultValue instanceof String) {
            return (T) conf.getProperty(key, (String) defaultValue);
        } else if (defaultValue instanceof Double) {
            return (T) new Double(conf.getDoubleProperty(key, ((Double) defaultValue).doubleValue()));
        } else if (defaultValue instanceof Boolean) {
            return (T) new Boolean(conf.getBooleanProperty(key, ((Boolean) defaultValue).booleanValue()));
        } else {
            throw new IllegalArgumentException("Not supported :" + defaultValue.getClass());
        }
    }

    public int getIntProperty(final String key, final int defaultValue) {
        final Optional<String> val = getProperty(key);
        return val.isPresent() ? Integer.parseInt(val.get()) : defaultValue;
    }

    public long getLongProperty(final String key, final long defaultValue) {
        final Optional<String> val = getProperty(key);
        return val.isPresent() ? Long.parseLong(val.get()) : defaultValue;
    }

    public double getDoubleProperty(final String key, final double defaultValue) {
        final Optional<String> val = getProperty(key);
        return val.isPresent() ? Double.parseDouble(val.get()) : defaultValue;
    }

    public boolean getBooleanProperty(final String key, final boolean defaultValue) {
        final Optional<String> val = getProperty(key);
        return val.isPresent() ? Boolean.parseBoolean(val.get()) : defaultValue;
    }

    private void parseConfigJson(final JsonNode jsonNode, final String prefix) {
        final Iterator<String> fieldNamesIt = jsonNode.fieldNames();
        while (fieldNamesIt.hasNext()) {
            final String fieldName = fieldNamesIt.next();
            final String newPrefix = prefix.isEmpty() ? fieldName.trim() : prefix + "." + fieldName.trim();
            final JsonNode newJsonNode = jsonNode.get(fieldName);
            if (newJsonNode.isObject()) {
                parseConfigJson(newJsonNode, newPrefix);
            } else {
                props.put(newPrefix, newJsonNode.asText().trim());
            }
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        this.props.entrySet().forEach(
            entry -> {
                sb.append(entry.getKey() + "<=>" + entry.getValue() + "\n");
            }
        );
        return sb.toString();
    }

    public Set<Object> getKeySet() {
        return this.props.keySet();
    }
}
