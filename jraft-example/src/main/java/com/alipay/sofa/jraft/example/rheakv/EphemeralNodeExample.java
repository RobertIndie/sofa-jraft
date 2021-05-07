/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alipay.sofa.jraft.example.rheakv;

import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.util.concurrent.DistributedLock;
import com.alipay.sofa.jraft.util.ExecutorServiceHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.alipay.sofa.jraft.util.BytesUtil.readUtf8;
import static com.alipay.sofa.jraft.util.BytesUtil.writeUtf8;

/**
 *
 * @author jiachun.fjc
 */
public class EphemeralNodeExample
{

    private static final Logger LOG = LoggerFactory.getLogger(EphemeralNodeExample.class);

    public static void main(final String[] args) throws Exception {
        final Client client = new Client();
        client.init();
        ephemeralPut(client.getRheaKVStore(), "testPath", writeUtf8("test_value"));
        client.shutdown();
    }

    public static void ephemeralPut(final RheaKVStore rheaKVStore, String path, byte[] value) {
        final ScheduledExecutorService watchdog = Executors.newSingleThreadScheduledExecutor();
        final DistributedLock<byte[]> lock = rheaKVStore.getDistributedLock(path, 3, TimeUnit.SECONDS, watchdog);
        if (lock.tryLock(value)) {
            try {
                LOG.info("Lock success with: {}", path);
            } finally {
                lock.unlock();
                LOG.info("Get value from lock: {}", readUtf8(lock.getOwnerContext()));
            }
        } else {
            LOG.info("Fail to lock with: {}", path);
        }
        // shutdown watchdog
        ExecutorServiceHelper.shutdownAndAwaitTermination(watchdog);
    }
}
