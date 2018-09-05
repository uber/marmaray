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


import com.google.common.base.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.uber.marmaray.common.util.AbstractSparkTest;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

public class TestConfigScopeResolver extends AbstractSparkTest {

    private String scopeOverrideMapKey;
    private String sampleYamlConfigFileName;
    private ConfigScopeResolver configOverrideResolver;

    @Before
    public void setup() {
        scopeOverrideMapKey = Configuration.SCOPE_OVERRIDE_MAPPING_KEY;
        sampleYamlConfigFileName = "/configWithScopes.yaml";
        configOverrideResolver = new ConfigScopeResolver(scopeOverrideMapKey);
    }

    @Test
    public void testConfigOverride() throws IOException {
        final JsonNode expectedWithBootstrapScopeResolution =
            getJsonNode("/expectedConfigWithBootstrapScope.yaml");
        final JsonNode actualWithBootstrapScopeResolution = configOverrideResolver
            .projectOverrideScopeOverDefault(Optional.of("bootstrap"),
                getJsonNode(sampleYamlConfigFileName));
        Assert
            .assertEquals(expectedWithBootstrapScopeResolution, actualWithBootstrapScopeResolution);

        final JsonNode expectedWithIncrementalScopeResolution =
            getJsonNode("/expectedConfigWithIncrementalScope.yaml");
        final JsonNode actualWithIncrementalScopeResolution = configOverrideResolver
            .projectOverrideScopeOverDefault(Optional.of("incremental"),
                getJsonNode(sampleYamlConfigFileName));
        Assert
            .assertEquals(expectedWithIncrementalScopeResolution,
                actualWithIncrementalScopeResolution);
    }

    @Test
    public void testNoConfigOverrideWhenScopeIsAbsent() throws IOException {
        final JsonNode expectedWithoutAnyScopeResolution =
            getJsonNode(sampleYamlConfigFileName);
        final JsonNode actualWithoutAnyScopeResolution = configOverrideResolver
            .projectOverrideScopeOverDefault(Optional.absent(),
                getJsonNode(sampleYamlConfigFileName));
        Assert
            .assertEquals(expectedWithoutAnyScopeResolution,
                actualWithoutAnyScopeResolution);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExceptionNonExistentScopeIsProvided() throws IOException {
        configOverrideResolver
            .projectOverrideScopeOverDefault(Optional.of("non-existent-scope"),
                getJsonNode(sampleYamlConfigFileName));
    }

    private JsonNode getJsonNode(final String resourceFileName) throws IOException {
        final InputStream yamlInputStream =
            TestConfigScopeResolver.class.getResourceAsStream(resourceFileName);
        final ObjectMapper yamlReader = new ObjectMapper(new YAMLFactory());
        return yamlReader.readTree(yamlInputStream);
    }
}
