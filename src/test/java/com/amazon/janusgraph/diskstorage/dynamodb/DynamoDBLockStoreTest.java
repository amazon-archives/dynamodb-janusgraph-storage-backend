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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.LockKeyColumnValueStoreTest;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
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
    public DynamoDBStoreManager openStorageManager(final int id /*ignore*/) throws BackendException {
        final List<String> storeNames = new ArrayList<>(2);
        storeNames.add(DB_NAME);
        storeNames.add(combination.getDataModel().name() + "_" + DB_NAME + "_lock_");
        storeNames.add(DB_NAME + "_lock_");
        storeNames.add("multi_store_lock_0");
        storeNames.add("multi_store_lock_1");
        storeNames.add("multi_store_lock_2");
        storeNames.add("multi_store_lock_3");
        storeNames.add("multi_store_lock_4");
        storeNames.add("multi_store_lock_5");
        final WriteConfiguration wc = TestGraphUtil.instance.getStoreConfig(combination.getDataModel(), storeNames);
        final ModifiableConfiguration config = new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS, wc,
            BasicConfiguration.Restriction.NONE);
        final boolean nativeLocking = combination.getUseNativeLocking();
        config.set(Constants.DYNAMODB_USE_NATIVE_LOCKING, nativeLocking);
        // TODO in JanusGraph: the configuration needs to get set and passed to store manager, otherwise the store manager will not be aware of it.
        // https://github.com/awslabs/dynamodb-titan-storage-backend/issues/160
        // BEGIN LockKeyColumnValueStoreTest code L115
        config.set(GraphDatabaseConfiguration.LOCK_LOCAL_MEDIATOR_GROUP, combination.toString() + id);
        config.set(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID,"inst" + id);
        config.set(GraphDatabaseConfiguration.LOCK_RETRY,10);
        config.set(GraphDatabaseConfiguration.LOCK_EXPIRE, Duration.ofMillis(EXPIRE_MS));
        // END LockKeyColumnValueStoreTest code L118

        return new DynamoDBStoreManager(config);
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
