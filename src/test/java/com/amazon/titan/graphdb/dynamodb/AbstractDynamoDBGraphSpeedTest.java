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
package com.amazon.titan.graphdb.dynamodb;

import com.amazon.titan.testutils.CiHeartbeat;
import org.junit.After;
import org.junit.AfterClass;

import com.amazon.titan.TestGraphUtil;
import com.amazon.titan.diskstorage.dynamodb.BackendDataModel;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.graphdb.SpeedTestSchema;
import com.thinkaurelius.titan.graphdb.TitanGraphSpeedTest;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

/**
 *
 * @author Alexander Patrikalakis
 * @author Johan Jacobs
 *
 */
public abstract class AbstractDynamoDBGraphSpeedTest extends TitanGraphSpeedTest {

    @Rule
    public final TestName testName = new TestName();

    private final CiHeartbeat ciHeartbeat;
    private static StandardTitanGraph graph;
    private static SpeedTestSchema schema;
    protected final BackendDataModel model;

    protected AbstractDynamoDBGraphSpeedTest(BackendDataModel model) throws BackendException {
        super(TestGraphUtil.instance().graphConfig(model));
        this.model = model;
        this.ciHeartbeat = new CiHeartbeat();
    }

    @AfterClass
    public static void deleteTables() throws BackendException {
        TestGraphUtil.instance().cleanUpTables();
    }

    @Override
    protected StandardTitanGraph getGraph() throws BackendException {
        if (null == graph) {
            GraphDatabaseConfiguration graphconfig = new GraphDatabaseConfiguration(conf);
            graphconfig.getBackend().clearStorage();
            graph = (StandardTitanGraph) TitanFactory.open(conf);
            initializeGraph(graph);
        }
        return graph;
    }

    @Override
    protected SpeedTestSchema getSchema() {
        if (null == schema) {
            schema = SpeedTestSchema.get();
        }
        return schema;
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
