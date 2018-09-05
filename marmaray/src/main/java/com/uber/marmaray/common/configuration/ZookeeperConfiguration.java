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

import com.uber.marmaray.utilities.ConfigUtil;
import lombok.Getter;
import lombok.NonNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * {@link ZookeeperConfiguration} defines zookeeper related configuration
 *
 * All properties start with {@link #ZK_PROPERTY_PREFIX}.
 */
public class ZookeeperConfiguration {
    public static final String ZK_PROPERTY_PREFIX = Configuration.MARMARAY_PREFIX + "zookeeper.";
    public static final String ZK_QUORUM = ZK_PROPERTY_PREFIX + "quorum";
    public static final String ZK_PORT = ZK_PROPERTY_PREFIX + "port";

    @Getter
    private final Configuration conf;
    @Getter
    private final String zkQuorum;
    @Getter
    private final String zkPort;

    public ZookeeperConfiguration(@NonNull final Configuration conf) {
        this.conf = conf;
        ConfigUtil.checkMandatoryProperties(this.getConf(), getMandatoryProperties());

        this.zkQuorum = this.getConf().getProperty(ZK_QUORUM).get();
        this.zkPort = this.getConf().getProperty(ZK_PORT).get();
    }

    public static List<String> getMandatoryProperties() {
        return Collections.unmodifiableList(Arrays.asList(ZK_QUORUM, ZK_PORT));
    }
}
