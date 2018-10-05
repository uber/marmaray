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

import org.hibernate.validator.constraints.NotEmpty;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link LockManagerConfiguration} defines configurations taking locks on jobs via ZooKeeper
 *
 * All properties start with {@link #LOCK_MANAGER_PREFIX}.
 */
@Slf4j
public class LockManagerConfiguration {

    public static final String LOCK_MANAGER_PREFIX = Configuration.MARMARAY_PREFIX + "lock_manager.";

    public static final String IS_ENABLED = LOCK_MANAGER_PREFIX + "is_enabled";
    public static final boolean DEFAULT_IS_ENABLED = true;

    public static final String ZK_BASE_PATH = LOCK_MANAGER_PREFIX + "zk_base_path";

    public static final String ZK_SESSION_TIMEOUT_MS = LOCK_MANAGER_PREFIX + "zk_session_timeout_ms";
    public static final int DEFAULT_ZK_SESSION_TIMEOUT_MS = 60 * 1000;

    public static final String ZK_CONNECTION_TIMEOUT_MS = LOCK_MANAGER_PREFIX + "zk_connection_timeout_ms";
    public static final int DEFAULT_ZK_CONNECTION_TIMEOUT_MS = 15 * 1000;

    public static final String ACQUIRE_LOCK_TIME_MS = LOCK_MANAGER_PREFIX + "acquire_lock_time_ms";
    public static final int DEFAULT_ACQUIRE_LOCK_TIME_MS = 60 * 1000;

    @Getter
    private final Configuration conf;

    @Getter
    private final boolean isEnabled;

    @Getter
    private final String zkBasePath;

    @Getter
    private final int zkSessionTimeoutMs;

    @Getter
    private final int zkConnectionTimeoutMs;

    @Getter
    private final int acquireLockTimeMs;

    public LockManagerConfiguration(@NonNull final Configuration conf) {
        this.conf = conf;
        this.isEnabled = this.getConf().getBooleanProperty(IS_ENABLED, DEFAULT_IS_ENABLED);
        if (this.isEnabled()) {
            ConfigUtil.checkMandatoryProperties(conf, getMandatoryProperties());
            this.zkBasePath = cleanZkBasePath(this.getConf().getProperty(ZK_BASE_PATH).get());
        } else {
            this.zkBasePath = null;
        }
        this.zkSessionTimeoutMs = this.getConf().getIntProperty(ZK_SESSION_TIMEOUT_MS, DEFAULT_ZK_SESSION_TIMEOUT_MS);
        this.zkConnectionTimeoutMs = this.getConf().getIntProperty(ZK_CONNECTION_TIMEOUT_MS,
                DEFAULT_ZK_CONNECTION_TIMEOUT_MS);
        this.acquireLockTimeMs = this.getConf().getIntProperty(ACQUIRE_LOCK_TIME_MS, DEFAULT_ACQUIRE_LOCK_TIME_MS);
    }

    private String cleanZkBasePath(@NotEmpty final String orig) {
        final String cleaned = orig.replaceAll("//*", "/").replaceAll("^/", "").replaceAll("/$", "");
        return cleaned;
    }

    private static List<String> getMandatoryProperties() {
        return Collections.unmodifiableList(Arrays.asList(ZK_BASE_PATH));
    }
}
