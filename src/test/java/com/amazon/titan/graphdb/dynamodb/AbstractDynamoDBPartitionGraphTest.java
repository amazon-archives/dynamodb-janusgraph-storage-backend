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

import java.time.Duration;
import java.util.Collections;

import com.thinkaurelius.titan.util.stats.MetricManager;
import org.junit.AfterClass;

import com.amazon.titan.TestGraphUtil;
import com.amazon.titan.diskstorage.dynamodb.BackendDataModel;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.TitanPartitionGraphTest;

/**
 *
 * @author Alexander Patrikalakis
 *
 */
public abstract class AbstractDynamoDBPartitionGraphTest extends TitanPartitionGraphTest
{
    protected final BackendDataModel model;
    protected AbstractDynamoDBPartitionGraphTest(BackendDataModel model) {
        this.model = model;
        MetricManager.INSTANCE.addConsoleReporter(Duration.ofSeconds(480));
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
}
