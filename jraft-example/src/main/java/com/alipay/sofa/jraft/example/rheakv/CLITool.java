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

import java.util.Arrays;
import java.util.stream.Collectors;

import static com.alipay.sofa.jraft.util.BytesUtil.readUtf8;
import static com.alipay.sofa.jraft.util.BytesUtil.writeUtf8;

public class CLITool {
    private static Client client;

    public static void main(final String[] args) throws Exception {
        client = new Client();
        client.init();
        final RheaKVStore rheaKVStore = client.getRheaKVStore();
        if (args.length < 1) {
            printHelp();
            shutdown(-1);
        }
        if (args[0].equals("get")) {
            if (args.length < 2) {
                System.out.println("Usage: get [key]");
                shutdown(-1);
            }
            System.out.printf("Get result from key %s : %s%n", args[1], readUtf8(rheaKVStore.bGet(args[1])));
            shutdown(0);
        }
        if (args[0].equals("put")) {
            if (args.length < 3) {
                System.out.println("Usage: put [key] [value]");
                shutdown(-1);
            }
            String value = Arrays.stream(args).skip(2).collect(Collectors.joining(" "));
            System.out.printf("Put result to key %s with value %s : %s %n", args[1], args[2],
                rheaKVStore.bPut(args[1], writeUtf8(value)));
            shutdown(0);
        }
        if (args[0].equals("list")) {
            final RheaIterator<KVEntry> it = rheaKVStore.iterator((String) null, null, 5);
            while (it.hasNext()) {
                final KVEntry kv = it.next();
                System.out.printf("%s=%s%n", readUtf8(kv.getKey()), readUtf8(kv.getValue()));
            }
            shutdown(0);
        }
        printHelp();
        shutdown(-1);
    }

    public static void shutdown(int status) {
        client.shutdown();
        System.exit(status);
    }

    public static void printHelp() {
        System.out.println("Usage: [get/set/list]\n" + "For example:\n" + "\tget mykey\n" + "\tput mykey simplevalue\n"
                           + "\tput mykey long value\n" + "\tlist");
    }
}
