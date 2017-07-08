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

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.graphdb.JanusGraphIterativeBenchmark;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import com.amazon.janusgraph.TestGraphUtil;
import com.amazon.janusgraph.diskstorage.dynamodb.BackendDataModel;
import com.amazon.janusgraph.diskstorage.dynamodb.DynamoDBStoreManager;
import com.amazon.janusgraph.testutils.CiHeartbeat;
import com.google.common.annotations.VisibleForTesting;

/**
 *
 * @author Alexander Patrikalakis
 * @author Johan Jacobs
 */
public abstract class AbstractDynamoDBGraphIterativeTest extends JanusGraphIterativeBenchmark {

    @Rule
    public final TestName testName = new TestName();

    private final CiHeartbeat ciHeartbeat;
    protected final BackendDataModel model;
    @VisibleForTesting
    protected AbstractDynamoDBGraphIterativeTest(final BackendDataModel model) {
        this.model = model;
        this.ciHeartbeat = new CiHeartbeat();
    }

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws BackendException {
        final WriteConfiguration wc = TestGraphUtil.instance.graphConfig(model);
        final BasicConfiguration config = new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS, wc,
            BasicConfiguration.Restriction.NONE);

        return new DynamoDBStoreManager(config);
    }

    @Override
    public WriteConfiguration getConfiguration()
    {
        return TestGraphUtil.instance.graphConfig(model);
    }

    @AfterClass
    public static void deleteTables() throws BackendException {
        TestGraphUtil.instance.cleanUpTables();
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
