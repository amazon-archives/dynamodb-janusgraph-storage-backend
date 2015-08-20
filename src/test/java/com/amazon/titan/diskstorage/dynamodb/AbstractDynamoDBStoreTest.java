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
package com.amazon.titan.diskstorage.dynamodb;

import java.util.Collections;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.junit.After;
import org.junit.Test;

import com.amazon.titan.diskstorage.dynamodb.test.TestGraphUtil;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;

/**
 *
 * @author Alexander Patrikalakis
 *
 */
public abstract class AbstractDynamoDBStoreTest extends KeyColumnValueStoreTest
{
    protected final BackendDataModel model;
    protected AbstractDynamoDBStoreTest(BackendDataModel model) {
        this.model = model;
    }
    @Override
    public KeyColumnValueStoreManager openStorageManager() throws StorageException
    {
        final List<String> extraStoreNames = Collections.singletonList("testStore1");
        final Configuration cc = TestGraphUtil.instance().getConfiguration(model, extraStoreNames);
        if(name.getMethodName().equals("parallelScanTest")) {
            cc.subset("storage").subset("dynamodb").addProperty(Constants.ENABLE_PARALLEL_SCAN, "true");
        }
        return new DynamoDBStoreManager(cc.subset(Constants.STORAGE_NS));
    }

    @Test
    public void parallelScanTest() throws Exception {
        this.scanTest();
    }

    @After
    public void cleanUpTables() throws Exception {
        TestGraphUtil.cleanUpTables();
    }
}
