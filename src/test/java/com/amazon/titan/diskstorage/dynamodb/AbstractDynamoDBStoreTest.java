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

import com.amazon.titan.testutils.TravisCiHeartbeat;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.amazon.titan.TestGraphUtil;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.rules.TestName;

/**
 *
 * @author Alexander Patrikalakis
 *
 */
public abstract class AbstractDynamoDBStoreTest extends KeyColumnValueStoreTest
{
    @Rule
    public TestName testName = new TestName();

    private TravisCiHeartbeat travisCiHeartbeat;
    protected final BackendDataModel model;
    protected AbstractDynamoDBStoreTest(BackendDataModel model) {
        this.model = model;
        this.travisCiHeartbeat = new TravisCiHeartbeat();
    }
    @Override
    public KeyColumnValueStoreManager openStorageManager() throws BackendException
    {
        final List<String> storeNames = Collections.singletonList("testStore1");
        final WriteConfiguration wc = TestGraphUtil.instance().getStoreConfig(model, storeNames);

        if (name.getMethodName().equals("parallelScanTest")) {
            wc.set("storage.dynamodb." + Constants.DYNAMODB_ENABLE_PARALLEL_SCAN.getName(), "true");
        }
        final BasicConfiguration config = new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS, wc,
            BasicConfiguration.Restriction.NONE);

        return new DynamoDBStoreManager(config);
    }

    @Test
    public void parallelScanTest() throws Exception {
        this.scanTest();
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();

        this.travisCiHeartbeat.startHeartbeat(this.testName.getMethodName());
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();

        TestGraphUtil.instance().cleanUpTables();
        this.travisCiHeartbeat.stopHeartBeat();
    }
}
