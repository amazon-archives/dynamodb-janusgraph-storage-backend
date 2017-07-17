/*
 * Copyright 2014-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazon.janusgraph.diskstorage.dynamodb;

import java.util.ArrayList;
import java.util.List;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.MultiWriteKeyColumnValueStoreTest;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.amazon.janusgraph.TestGraphUtil;
import com.amazon.janusgraph.testutils.CiHeartbeat;

/**
 *
 * @author Alexander Patrikalakis
 * @author Johan Jacobs
 *
 */
public abstract class AbstractDynamoDBMultiWriteStoreTest extends MultiWriteKeyColumnValueStoreTest {

    @Rule
    public final TestName testName = new TestName();

    private final CiHeartbeat ciHeartbeat;
    protected final BackendDataModel model;
    protected AbstractDynamoDBMultiWriteStoreTest(final BackendDataModel model) {
        this.model = model;
        this.ciHeartbeat = new CiHeartbeat();
    }

    public KeyColumnValueStoreManager openStorageManager() throws BackendException {
        final List<String> storeNames = new ArrayList<>(2);
        storeNames.add(storeName1);
        storeNames.add(storeName2);
        final WriteConfiguration wc = TestGraphUtil.instance.getStoreConfig(model, storeNames);
        final BasicConfiguration config = new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS, wc,
            BasicConfiguration.Restriction.NONE);

        return new DynamoDBStoreManager(config);
    }

    @AfterClass
    public static void cleanUpTables() throws Exception {
        TestGraphUtil.instance.cleanUpTables();
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.ciHeartbeat.startHeartbeat(this.testName.getMethodName());
    }

    @After
    public void tearDown() throws Exception {
        this.ciHeartbeat.stopHeartbeat();
        super.tearDown();
    }

    @Override
    @Test
    public void mutateManyStressTest() throws BackendException {
        this.mutateManyStressTestWithVariableRounds(1);
    }
}
