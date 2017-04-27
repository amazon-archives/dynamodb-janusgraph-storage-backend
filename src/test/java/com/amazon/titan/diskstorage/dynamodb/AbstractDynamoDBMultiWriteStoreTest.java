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

import java.util.ArrayList;
import java.util.List;

import com.amazon.titan.testutils.TravisCiHeartbeat;
import org.junit.After;
import org.junit.AfterClass;

import com.amazon.titan.TestGraphUtil;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.MultiWriteKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

/**
*
* @author Alexander Patrikalakis
*
*/
public abstract class AbstractDynamoDBMultiWriteStoreTest extends MultiWriteKeyColumnValueStoreTest {

    @Rule
    public TestName testName = new TestName();

    private TravisCiHeartbeat travisCiHeartbeat;
    protected final BackendDataModel model;
    protected AbstractDynamoDBMultiWriteStoreTest(BackendDataModel model) {
        this.model = model;
        this.travisCiHeartbeat = new TravisCiHeartbeat();
    }

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws BackendException {
        final List<String> storeNames = new ArrayList<>(2);
        storeNames.add("testStore1");
        storeNames.add("testStore2");
        final WriteConfiguration wc = TestGraphUtil.instance().getStoreConfig(model, storeNames);
        final BasicConfiguration config = new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS, wc,
            BasicConfiguration.Restriction.NONE);

        return new DynamoDBStoreManager(config);
    }

    @AfterClass
    public static void cleanUpTables() throws Exception {
        TestGraphUtil.instance().cleanUpTables();
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();

        this.travisCiHeartbeat.startHeartbeat(this.testName.getMethodName());
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();

        this.travisCiHeartbeat.stopHeartBeat();
    }
}
