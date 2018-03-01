/*
 * Copyright 2014-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import java.util.Collection;
import java.util.List;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.LockKeyColumnValueStoreTest;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.MergedConfiguration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.amazon.janusgraph.TestGraphUtil;
import com.amazon.janusgraph.graphdb.dynamodb.TestCombination;
import com.amazon.janusgraph.testcategory.IsolateRemainingTestsCategory;
import com.amazon.janusgraph.testutils.CiHeartbeat;
import com.google.common.collect.ImmutableList;

/**
 *
 * @author Alexander Patrikalakis
 * @author Johan Jacobs
 *
 */
@Category({IsolateRemainingTestsCategory.class})
@RunWith(Parameterized.class)
public class DynamoDBLockStoreTest extends LockKeyColumnValueStoreTest {

    @Rule
    public final TestName testName = new TestName();

    private final CiHeartbeat ciHeartbeat;

    //TODO
    @Parameterized.Parameters//(name = "{0}")
    public static Collection<Object[]> data() {
        return TestCombination.LOCKING_CROSS_MODELS;
    }
    private final TestCombination combination;
    public DynamoDBLockStoreTest(final TestCombination combination) {
        this.ciHeartbeat = new CiHeartbeat();
        this.combination = combination;
    }

    @Override
    public DynamoDBStoreManager openStorageManager(final int id, Configuration configuration) throws BackendException {

        final List<String> storeNames = ImmutableList.of(DB_NAME,
            combination.getDataModel().name() + "_" + DB_NAME + "_lock_",
            DB_NAME + "_lock_",
            "multi_store_lock_0",
            "multi_store_lock_1",
            "multi_store_lock_2",
            "multi_store_lock_3",
            "multi_store_lock_4",
            "multi_store_lock_5");
        final ModifiableConfiguration dynamodbOverrides = new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS,
            TestGraphUtil.instance.getStoreConfig(combination.getDataModel(), storeNames),
            BasicConfiguration.Restriction.NONE);
        dynamodbOverrides.set(Constants.DYNAMODB_USE_NATIVE_LOCKING, combination.getUseNativeLocking());
        dynamodbOverrides.set(GraphDatabaseConfiguration.LOCK_LOCAL_MEDIATOR_GROUP, combination.toString() + id);
        return new DynamoDBStoreManager(new MergedConfiguration(dynamodbOverrides, configuration));
    }

    @Before
    public void setUpTest() throws Exception {
        this.ciHeartbeat.startHeartbeat(this.testName.getMethodName());
        // https://github.com/awslabs/dynamodb-titan-storage-backend/issues/160
        // super.open() is called here, and super.open() calls super.openStoreManager(int)
        super.setUp();
    }

    @After
    public void tearDownTest() throws Exception {
        super.tearDown();
        TestGraphUtil.instance.cleanUpTables();
        this.ciHeartbeat.stopHeartbeat();
    }

    @Override
    @Test
    public void testRemoteLockContention() throws InterruptedException, BackendException {
        //The DynamoDB Storage Backend for Titan does not support remote lock expiry currently.
        //Re-read the KeyColumns (edges, vertices, index entries) and retry.
        //If you enable a DynamoDB Stream on the store (table) and have a Lambda function
        //write the stream to a Kinesis stream, and then use KCL inside the AbstractDynamoDbStore,
        //it may be possible to support remote lock expiry in the future. To not confuse transactions
        //in process on the same host, each item would need to write the machine id to a metadata
        //attribute to each column, so the KCL application on the Titan nodes can selectively
        //ignore key-column writes originating from the same node. DynamoDB Streams fan-out is
        //only twice the number of writes, so to support remote lock expiry on more than two writers
        //that are saturating the write capacity of the store, you need to stream the records through
        //Kinesis and configure the fan-out you need for all your Titan writers. Adding Streams +
        //Lambda + Kinesis + KCL to the loop may add around 1sec to remote lock expiry latency.
        //This is partially quantifiable with the approximate time attribute added to Kinesis
        //record format recently.
        if (combination.getUseNativeLocking()) { //
            return;

        }
        super.testRemoteLockContention();
    }
}
