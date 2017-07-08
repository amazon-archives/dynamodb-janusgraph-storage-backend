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
package com.amazon.janusgraph.diskstorage.dynamodb;

import java.util.ArrayList;
import java.util.List;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.log.KCVSLogTest;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.AfterClass;

import com.amazon.janusgraph.TestGraphUtil;

/**
 *
 * @author Alexander Patrikalakis
 * @author Johan Jacobs
 */
public abstract class AbstractDynamoDBLogTest extends KCVSLogTest {

    protected final BackendDataModel model;
    protected AbstractDynamoDBLogTest(final BackendDataModel model) {
        this.model = model;
    }

    public KeyColumnValueStoreManager openStorageManager() throws BackendException {
        final List<String> logNames = new ArrayList<>(12);
        logNames.add("test1");
        logNames.add("durable");
        logNames.add("ml0");
        logNames.add("ml1");
        logNames.add("ml2");
        logNames.add("loner0");
        logNames.add("loner1");
        logNames.add("loner2");
        logNames.add("loner3");
        logNames.add("loner4");
        logNames.add("fuzz");
        logNames.add("testx");
        final WriteConfiguration wc = TestGraphUtil.instance.getStoreConfig(model, logNames);
        final BasicConfiguration config = new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS, wc,
            BasicConfiguration.Restriction.NONE);

        return new DynamoDBStoreManager(config);
    }

    @AfterClass
    public static void cleanUpTables() throws Exception {
        TestGraphUtil.instance.cleanUpTables();
    }
}
