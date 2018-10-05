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
import com.uber.marmaray.common.configuration.LockManagerConfiguration;
import com.uber.marmaray.common.configuration.ZookeeperConfiguration;
import com.uber.marmaray.common.exceptions.JobRuntimeException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.hibernate.validator.constraints.NotEmpty;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Optional;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link LockManager} manages locks via zookeeper. Internally, a map between names and locks are
 * maintained. This class implements {@link AutoCloseable} interfaces, so it will be automatically
 * closed if used as a resource in try blocks.
 *
 * {@link #getLockKey(String...)} generates one single lock key based on the list of strings. {@link
 * #lock(String, String)} and {@link #unlock(String)} accquires and releases the lock with the given
 * name. {@link #batchLock(List, String)} acquire locks a list of lock name strings. It only
 * succeeds and keeps all the locks if all the acquiring were successful.
 */
@Slf4j
public class LockManager implements AutoCloseable {
    @NonNull
    private final ConcurrentHashMap<String, CustomizedInterProcessMutex> lockMap;
    @NonNull
    private Optional<CuratorFramework> client;
    @NonNull
    private final LockManagerConfiguration lockConf;

    private final boolean isEnabled;

    public LockManager(@NonNull final Configuration conf) {
        this.lockConf = new LockManagerConfiguration(conf);
        this.isEnabled = lockConf.isEnabled();
        this.lockMap = new ConcurrentHashMap();
        if (this.isEnabled) {
            final ZookeeperConfiguration zkConf = new ZookeeperConfiguration(conf);
            this.client = Optional.of(CuratorFrameworkFactory.builder()
                    .connectString(zkConf.getZkQuorum())
                    .retryPolicy(new BoundedExponentialBackoffRetry(1000, 5000, 3))
                    .namespace(lockConf.getZkBasePath())
                    .sessionTimeoutMs(lockConf.getZkSessionTimeoutMs())
                    .connectionTimeoutMs(lockConf.getZkConnectionTimeoutMs())
                    .build());
            this.client.get().start();
        } else {
            this.client = Optional.absent();
        }
    }

    public static String getLockKey(@NotEmpty final String... keys) {
        return "/" + String.join("/", keys);
    }

    public boolean lock(@NotEmpty final String lockKey, final String lockInfo) throws JobRuntimeException {
        if (!this.isEnabled) {
            log.info("The LockManager is not enabled. Consider the key {} locked.", lockKey);
            return true;
        }
        if (this.lockMap.containsKey(lockKey)) {
            log.info("The existing lock {} has info {}", lockKey,
                    new String(this.lockMap.get(lockKey).getLockNodeBytes()));
            return this.lockMap.get(lockKey).isAcquiredInThisProcess();
        } else {
            log.info("Acquiring a new lock for {}", lockKey);
            final CustomizedInterProcessMutex newLockValue = new CustomizedInterProcessMutex(
                    client.get(), lockKey, lockInfo);
            try {
                if (acquireLock(newLockValue)) {
                    log.info("Acquired a new lock for {}", lockKey);
                    lockMap.put(lockKey, newLockValue);
                    return true;
                } else {
                    log.info("Unable to acquire a new lock for {}", lockKey);
                    return false;
                }
            } catch (Exception e) {
                throw new JobRuntimeException(String.format("Failed to acquire a new lock for %s", lockKey), e);
            }
        }
    }

    public boolean unlock(@NotEmpty final String lockKey) throws JobRuntimeException {
        if (!this.isEnabled) {
            log.info("The LockManager is not enabled. Consider the key {} unlocked.", lockKey);
            return true;
        }
        if (!this.lockMap.containsKey(lockKey)) {
            log.error("This lock has not been acquired yet: {}", lockKey);
            throw new JobRuntimeException(String.format("Failed to unlock {}: not acquired.", lockKey));
        }
        final CustomizedInterProcessMutex lockValue = lockMap.get(lockKey);
        if (!lockValue.isAcquiredInThisProcess()) {
            log.error("This lock was not acquired by this job: {}", lockKey);
            throw new JobRuntimeException(String.format("Failed to unlock %s: not acquired by this job.", lockKey));
        }
        try {
            log.info("Releasing the lock for {}", lockKey);
            lockValue.release();
            lockMap.remove(lockKey);
            return true;
        } catch (Exception e) {
            throw new JobRuntimeException(String.format("Unbable to unlock %s", lockKey), e);
        }
    }

    public boolean batchLock(@NotEmpty final List<String> lockKeyList,
                             final String lockInfo)
            throws JobRuntimeException {

        final List<String> locked = new ArrayList();
        final AtomicBoolean isSuccess = new AtomicBoolean(true);
        try {
            for (final String lockKey : lockKeyList) {
                if (this.lock(lockKey, lockInfo)) {
                    locked.add(lockKey);
                } else {
                    isSuccess.set(false);
                    break;
                }
            }
        } catch (Exception e) {
            isSuccess.set(false);
            new JobRuntimeException("Failed to lock all keys at once", e);
        } finally {
            if (!isSuccess.get()) {
                log.info("Failed to lock all keys at once, will release them.");
                log.info("The requested lock list is " + lockKeyList);
                log.info("The locked list is " + locked);
                for (final String lockKey : locked) {
                    this.unlock(lockKey);
                }
            }
            return isSuccess.get();
        }
    }

    @Override
    public void close() {
        if (!this.isEnabled) {
            log.info("The LockManager is not enabled. Closing it anyways.");
            return;
        }
        lockMap.forEach((key, mutex) -> {
                try {
                    unlock(key);
                    lockMap.remove(key);
                } catch (Exception e) {
                    throw new JobRuntimeException("Failed to close the LockManager", e);
                }
            }
        );
        if (client.isPresent()) {
            client.get().close();
            client = Optional.absent();
        }
    }

    private boolean acquireLock(@NonNull final InterProcessMutex lock) throws Exception {
        return lock.acquire(lockConf.getAcquireLockTimeMs(), TimeUnit.MILLISECONDS);
    }

    private final class CustomizedInterProcessMutex extends InterProcessMutex {

        private final byte[] lockInfo;

        private CustomizedInterProcessMutex(@NonNull final CuratorFramework client,
                                            @NotEmpty final String path,
                                            final String info) {
            super(client, path);
            if (info.equals("")) {
                this.lockInfo = null;
            } else {
                this.lockInfo = info.getBytes();
            }
        }

        @Override
        protected byte[] getLockNodeBytes() {
            return this.lockInfo;
        }
    }
}
