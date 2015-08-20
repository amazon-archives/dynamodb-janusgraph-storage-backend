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

import java.util.Collections;

import org.junit.AfterClass;

import com.amazon.titan.diskstorage.dynamodb.BackendDataModel;
import com.amazon.titan.diskstorage.dynamodb.test.TestGraphUtil;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.TitanGraphConcurrentTest;

/**
 *
 * @author Alexander Patrikalakis
 *
 */
public abstract class AbstractDynamoDBGraphConcurrentTest extends TitanGraphConcurrentTest
{
    protected final BackendDataModel model;
    protected AbstractDynamoDBGraphConcurrentTest(BackendDataModel model) {
        this.model = model;
    }

    @Override
    public WriteConfiguration getConfiguration()
    {
        return TestGraphUtil.instance().getWriteConfiguration(model, Collections.<String>emptyList());
    }

    @AfterClass
    public static void deleteTables() throws BackendException {
        TestGraphUtil.cleanUpTables();
    }
}
