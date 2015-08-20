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

import static org.junit.Assert.assertNotNull;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import com.amazon.titan.diskstorage.dynamodb.test.TestGraphUtil;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration.Restriction;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.configuration.backend.CommonsConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

/**
 *
 * @author Matthew Sowders
 * @author Alexander Patrikalakis
 */
public class ClientTest {
    private Client client;

    private Configuration titanConfig;

    @Before
    public void setUp() {
        titanConfig = new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS,
                                             new CommonsConfiguration(TestGraphUtil.loadProperties()),
                                             Restriction.NONE);
        client = new Client(titanConfig);
    }

    @Test
    public void shutdown() throws Exception {
        Client client = TestGraphUtil.createClient();
        client.delegate().shutdown();
    }

    @AfterClass
    public static void cleanUpTables() throws Exception {
        TestGraphUtil.cleanUpTables();
    }

    @Test
    public void client() throws Exception {
        AmazonDynamoDB dynamoDBAsyncClient = client.delegate().client();
        assertNotNull(dynamoDBAsyncClient);
    }
}
