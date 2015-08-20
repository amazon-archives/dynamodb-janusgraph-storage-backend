/*
 * Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazon.titan.diskstorage.dynamodb.test;

import com.amazon.titan.diskstorage.dynamodb.DynamoDBStoreTransaction;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTxConfig;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;

/**
 *
 * @author Matthew Sowders
 */
public class TestValues {

    private TestValues() {
        // nop - constants only
    }

    public static StaticBuffer createKey() {
        return createKey("test_key");
    }

    public static StaticBuffer createKey(String key) {
        return new StaticArrayBuffer(key.getBytes());
    }

    public static StaticBuffer createValue() {
        return new StaticArrayBuffer("test_value".getBytes());
    }

    public static KeyValueEntry createNewItem(String key) {
        StaticBuffer keyBuffer = createKey(key);
        StaticBuffer value = createValue();
        return new KeyValueEntry(keyBuffer, value);
    }

    public static byte[] createRid() {
        return "rid".getBytes();
    }

    public static StoreTransaction createTransaction() {
        return new DynamoDBStoreTransaction(new StoreTxConfig(), createRid());
    }
}
