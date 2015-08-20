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
import org.junit.Test;

import com.amazon.titan.diskstorage.dynamodb.test.TestGraphUtil;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;

/**
 *
 * @author Matthew Sowders
 * @author Alexander Patrikalakis
 */
public class ClientTest {
    @Test
    public void shutdown() throws Exception {
        Client client = TestGraphUtil.createClient();
        client.delegate().shutdown();
    }

    /**
     * TODO for some reason this class creates SINGLE tables as well, check that this is OK.
     */
    @AfterClass
    public static void cleanUpTables() throws Exception {
        Client client = TestGraphUtil.createClient();
        ListTablesResult result = client.delegate().listAllTables();
        for(String tableName : result.getTableNames()) {
            try {
                client.delegate().deleteTable(new DeleteTableRequest().withTableName(tableName));
            } catch (ResourceNotFoundException e) {
                // It's possible that the table creation failed, so we should swallow this exception
                System.err.println("Could not delete table: " + tableName + " because it didn't exist");
            }
        }
    }

    @Test
    public void client() throws Exception {
        Client client = TestGraphUtil.createClient();
        AmazonDynamoDB dynamoDBAsyncClient = client.delegate().client();
        assertNotNull(dynamoDBAsyncClient);
    }

}
