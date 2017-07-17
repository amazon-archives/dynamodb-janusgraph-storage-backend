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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.KeyColumnValueStoreTest;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import com.amazon.janusgraph.TestGraphUtil;
import com.amazon.janusgraph.testutils.CiHeartbeat;

/**
 *
 * @author Alexander Patrikalakis
 * @author Johan Jacobs
 *
 */
public abstract class AbstractDynamoDbStoreTest extends KeyColumnValueStoreTest
{
    @Rule
    public final TestName testName = new TestName();

    private final int NUM_COLUMNS = 50;
    private final CiHeartbeat ciHeartbeat;
    protected final BackendDataModel model;
    protected AbstractDynamoDbStoreTest(final BackendDataModel model) {
        this.model = model;
        this.ciHeartbeat = new CiHeartbeat();
    }
    @Override
    public KeyColumnValueStoreManager openStorageManager() throws BackendException
    {
        final List<String> storeNames = Collections.singletonList("testStore1");
        final WriteConfiguration wc = TestGraphUtil.instance.getStoreConfig(model, storeNames);

        if (name.getMethodName().equals("parallelScanTest")) {
            wc.set("storage.dynamodb." + Constants.DYNAMODB_ENABLE_PARALLEL_SCAN.getName(), "true");
        }
        final BasicConfiguration config = new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS, wc,
            BasicConfiguration.Restriction.NONE);

        return new DynamoDBStoreManager(config);
    }

    @Override
    public void testConcurrentGetSliceAndMutate() throws ExecutionException, InterruptedException, BackendException {
        testConcurrentStoreOps(true, NUM_COLUMNS);
    }

    @Override
    public void testConcurrentGetSlice() throws ExecutionException, InterruptedException, BackendException {
        testConcurrentStoreOps(false, NUM_COLUMNS);
    }
    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.ciHeartbeat.startHeartbeat(this.testName.getMethodName());
    }

    @After
    public void tearDown() throws Exception {
        TestGraphUtil.instance.cleanUpTables();
        this.ciHeartbeat.stopHeartbeat();
        super.tearDown();
    }
}
