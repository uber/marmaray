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

import com.uber.marmaray.common.configuration.Configuration;
import com.uber.marmaray.common.configuration.LockManagerConfiguration;
import com.uber.marmaray.common.configuration.ZookeeperConfiguration;
import com.uber.marmaray.utilities.LockManager;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;


import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Slf4j
public class TestLockManager {

    @NonNull
    private static final Configuration conf = new Configuration();

    @NonNull
    private static TestingServer zkServer;

    @BeforeClass
    public static void startZK() throws Exception {
        zkServer = new TestingServer();
        zkServer.start();
        conf.setProperty(LockManagerConfiguration.IS_ENABLED, "true");
        conf.setProperty(LockManagerConfiguration.ZK_BASE_PATH, "/////test/////lock_manager////");
        conf.setProperty(ZookeeperConfiguration.ZK_QUORUM, zkServer.getConnectString());
        conf.setProperty(ZookeeperConfiguration.ZK_PORT, Integer.toString(zkServer.getPort()));
        conf.setProperty(LockManagerConfiguration.ACQUIRE_LOCK_TIME_MS, Integer.toString(10 * 1000));
        conf.setProperty(LockManagerConfiguration.ZK_SESSION_TIMEOUT_MS, Integer.toString(30 * 1000));
        conf.setProperty(LockManagerConfiguration.ZK_CONNECTION_TIMEOUT_MS, Integer.toString(12 * 1000));
    }

    @AfterClass
    public static void closeZK() throws Exception {
        zkServer.close();
    }

    @Test
    public void testLockKeyGen() throws Exception {
        final String lockKey = LockManager.getLockKey("part1", "part2", "part3");
        assertEquals("/part1/part2/part3", lockKey);
    }

    @Test
    public void testLockHappyUser() throws Exception {

        final MultiThreadTestCoordinator coordinator = new MultiThreadTestCoordinator();
        final String key1 = LockManager.getLockKey("test_lock", "key1");

        final Callable<Void> task1 = new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                try (final LockManager lockManager = new LockManager(conf)) {
                    coordinator.waitUntilStep(0);
                    assertTrue(lockManager.lock(key1, ""));
                    coordinator.nextStep();
                    coordinator.waitUntilStep(2);
                    assertTrue(lockManager.unlock(key1));
                } finally {
                    coordinator.nextStep();
                    return null;
                }
            }
        };

        final Callable<Void> task2 = new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                try (final LockManager lockManager = new LockManager(conf)) {
                    coordinator.waitUntilStep(1);
                    assertFalse(lockManager.lock(key1, ""));
                    coordinator.nextStep();
                    coordinator.waitUntilStep(3);
                    assertTrue(lockManager.lock(key1, ""));
                } finally {
                    coordinator.nextStep();
                    return null;
                }
            }
        };

        final ExecutorService service = Executors.newFixedThreadPool(2);
        final Future<Void> result1 = service.submit(task1);
        final Future<Void> result2 = service.submit(task2);

        service.shutdown();
        service.awaitTermination(2, TimeUnit.MINUTES);

        result1.get();
        result2.get();

        final LockManager lockManager = new LockManager(conf);
        coordinator.waitUntilStep(4);
        assertTrue(lockManager.lock(key1, ""));
        lockManager.close();
    }

    @Test
    public void testLockLostNode() throws Exception {
        final MultiThreadTestCoordinator coordinator = new MultiThreadTestCoordinator();
        final String key2 = LockManager.getLockKey("test_lock", "key2");
        final String FAIL_MESSAGE = "Failing on purpose";

        final Callable<Void> task1 = new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                final LockManager lockManager = new LockManager(conf);
                try {
                    coordinator.waitUntilStep(0);
                    assertTrue(lockManager.lock(key2, ""));
                    coordinator.nextStep();
                    coordinator.waitUntilStep(2);
                    throw new Exception(FAIL_MESSAGE);
                } finally {
                    lockManager.close();
                    coordinator.nextStep();
                }
            }
        };

        final Callable<Void> task2 = new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                try (final LockManager lockManager = new LockManager(conf)) {
                    coordinator.waitUntilStep(1);
                    assertFalse(lockManager.lock(key2, ""));
                    coordinator.nextStep();
                    coordinator.waitUntilStep(3);
                    assertTrue(lockManager.lock(key2, ""));
                    coordinator.nextStep();
                    return null;
                }
            }
        };

        final ExecutorService service = Executors.newFixedThreadPool(2);

        final Future<Void> result1 = service.submit(task1);
        final Future<Void> result2 = service.submit(task2);

        service.shutdown();
        service.awaitTermination(2, TimeUnit.MINUTES);

        try {
            result1.get();
            fail("The first task should fail on purpose.");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(FAIL_MESSAGE));
        }
        result2.get();
    }

    @Test
    public void testBatchLock() throws Exception {
        final String key1 = LockManager.getLockKey("test_lock", "batchkey1");
        final String key2 = LockManager.getLockKey("test_lock", "batchkey2");
        final List<String> lockKeyList = Arrays.asList(key1, key2);

        try (final LockManager lockManager = new LockManager(conf)) {
            assertTrue(lockManager.batchLock(lockKeyList, ""));
        }

        final MultiThreadTestCoordinator coordinator = new MultiThreadTestCoordinator();


        // Order of these three tasks
        // Step 0. task1: lock(key1)-success
        // Step 1. task2: lock(key1,key2)-failure
        // Step 2. task3: lock(key2)-success
        //         task3: unlock(key2)
        // Step 3. task1: unlock(key1)
        // Step 4. task2: lock(key1,key2)-success

        final Callable<Void> task1 = new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                try (final LockManager lockManager = new LockManager(conf)) {
                    coordinator.waitUntilStep(0);
                    assertTrue(lockManager.lock(key1, ""));
                    coordinator.nextStep();
                    coordinator.waitUntilStep(3);
                }
                coordinator.nextStep();
                return null;
            }
        };

        final Callable<Void> task2 = new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                try (final LockManager lockManager = new LockManager(conf)) {
                    coordinator.waitUntilStep(1);
                    assertFalse(lockManager.batchLock(lockKeyList, ""));
                    coordinator.nextStep();
                    coordinator.waitUntilStep(4);
                    assertTrue(lockManager.batchLock(lockKeyList, ""));
                    coordinator.nextStep();
                }
                return null;
            }
        };

        final Callable<Void> task3 = new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                try (final LockManager lockManager = new LockManager(conf)) {
                    coordinator.waitUntilStep(2);
                    assertTrue(lockManager.lock(key2,""));
                }
                coordinator.nextStep();
                return null;
            }
        };

        final ExecutorService service = Executors.newFixedThreadPool(3);

        final Future<Void> result1 = service.submit(task1);
        final Future<Void> result2 = service.submit(task2);
        final Future<Void> result3 = service.submit(task3);

        service.shutdown();
        service.awaitTermination(2, TimeUnit.MINUTES);

        result1.get();
        result2.get();
        result3.get();
    }

    @Test
    public void testLockInfo() throws Exception {

        final String LOCK_INFO = "This is the information in the lock";
        final String key1 = LockManager.getLockKey("test_lock", "key1");

        try (final LockManager lockManager = new LockManager(conf)) {

            assertTrue(lockManager.lock(key1, LOCK_INFO));

            final LockManagerConfiguration lockConf = new LockManagerConfiguration(conf);
            final ZookeeperConfiguration zkConf = new ZookeeperConfiguration(conf);
            final CuratorFramework client = CuratorFrameworkFactory.builder()
                    .connectString(zkConf.getZkQuorum())
                    .retryPolicy(new BoundedExponentialBackoffRetry(1000, 5000, 3))
                    .namespace(lockConf.getZkBasePath())
                    .sessionTimeoutMs(lockConf.getZkSessionTimeoutMs())
                    .connectionTimeoutMs(lockConf.getZkConnectionTimeoutMs())
                    .build();

            client.start();
            final String node = client.getChildren().forPath(key1).get(0);
            final String data = new String(client.getData().forPath(key1 + "/" + node));
            assertEquals(LOCK_INFO, data);
            client.close();

            lockManager.unlock(key1);
        }
    }

}
