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
package com.amazon.janusgraph.graphdb.dynamodb;

import java.util.Collections;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphPartitionGraphTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import com.amazon.janusgraph.TestGraphUtil;
import com.amazon.janusgraph.diskstorage.dynamodb.BackendDataModel;
import com.amazon.janusgraph.testutils.CiHeartbeat;

/**
 *
 * @author Alexander Patrikalakis
 * @author Johan Jacobs
 *
 */
public abstract class AbstractDynamoDBPartitionGraphTest extends JanusGraphPartitionGraphTest
{

    @Rule
    public final TestName testName = new TestName();

    private final CiHeartbeat ciHeartbeat;
    protected final BackendDataModel model;
    protected AbstractDynamoDBPartitionGraphTest(BackendDataModel model) {
        this.model = model;
        this.ciHeartbeat = new CiHeartbeat();
    }

    @Override
    public WriteConfiguration getBaseConfiguration()
    {
        return TestGraphUtil.instance().graphConfigWithClusterPartitionsAndExtraStores(model,
            Collections.<String>emptyList(), 8 /*titanClusterPartitions*/);
    }

    @AfterClass
    public static void deleteTables() throws BackendException {
        TestGraphUtil.instance().cleanUpTables();
    }

    @Before
    public void setUpTest() throws Exception {
        this.ciHeartbeat.startHeartbeat(this.testName.getMethodName());
    }

    @After
    public void tearDownTest() throws Exception {
        this.ciHeartbeat.stopHeartbeat();
    }
}
