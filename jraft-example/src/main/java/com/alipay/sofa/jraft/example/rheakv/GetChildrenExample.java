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

import com.alipay.sofa.jraft.rhea.client.RheaIterator;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.rhea.util.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.alipay.sofa.jraft.util.BytesUtil.readUtf8;
import static com.alipay.sofa.jraft.util.BytesUtil.writeUtf8;

/**
 *
 * @author jiachun.fjc
 */
public class GetChildrenExample
{

    private static final Logger LOG = LoggerFactory.getLogger(GetChildrenExample.class);

    public static void main(final String[] args) throws Exception {
        final Client client = new Client();
        client.init();
        testGetChildren(client.getRheaKVStore());
        client.shutdown();
    }

    public static void testGetChildren(final RheaKVStore rheaKVStore) {
        final List<byte[]> keys = Lists.newArrayList();
        rheaKVStore.bPut("public/other", writeUtf8("Test"));
        for (int i = 0; i < 10; i++) {
            final byte[] bytes = writeUtf8("public/default/" + i);
            keys.add(bytes);
            rheaKVStore.bPut(bytes, bytes);
        }
        rheaKVStore.bPut("public/default/other", writeUtf8("Test"));

        List<String> children = getChildren("public/default", rheaKVStore);
        for (String key : children) {
            LOG.info("Get children: {}", key);
        }
    }

    public static List<String> getChildren(String path, RheaKVStore rheaKVStore) {
        List<String> result = Lists.newArrayList();
        final RheaIterator<KVEntry> it = rheaKVStore.iterator(path, null, 5);
        while (it.hasNext()) {
            final KVEntry kv = it.next();
            String key = readUtf8(kv.getKey());
            if (key.startsWith(path)) {
                result.add(key);
            }
        }
        return result;
    }
}
