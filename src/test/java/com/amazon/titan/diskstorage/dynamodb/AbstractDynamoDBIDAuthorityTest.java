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

import com.amazon.titan.testutils.CiHeartbeat;
import org.junit.After;

import com.amazon.titan.TestGraphUtil;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.IDAuthorityTest;
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
 * @author Johan Jacobs
 *
 */
public abstract class AbstractDynamoDBIDAuthorityTest extends IDAuthorityTest {

    @Rule
    public final TestName testName = new TestName();

    private final CiHeartbeat ciHeartbeat;

    protected final BackendDataModel model;
    protected AbstractDynamoDBIDAuthorityTest(WriteConfiguration baseConfig,
            BackendDataModel model) {
        super(TestGraphUtil.instance().appendStoreConfig(model, baseConfig.copy(), Collections.singletonList("ids")));
        this.model = model;
        this.ciHeartbeat = new CiHeartbeat();
    }
    
    @Override
    public KeyColumnValueStoreManager openStorageManager() throws BackendException {
        final BasicConfiguration config = new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS, super.baseStoreConfiguration,
            BasicConfiguration.Restriction.NONE);
        return new DynamoDBStoreManager(config);
    }

    @Before
    public void setUpTest() throws Exception {
        this.ciHeartbeat.startHeartbeat(this.testName.getMethodName());
    }

    @After
    public void tearDownTest() throws Exception {
        TestGraphUtil.instance().cleanUpTables();
        this.ciHeartbeat.stopHeartbeat();
    }
}
