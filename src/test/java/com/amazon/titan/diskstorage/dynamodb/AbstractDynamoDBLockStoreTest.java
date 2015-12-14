/*
 * Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Portions copyright Titan: Distributed Graph Database - Copyright 2012 and onwards Aurelius.
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Ignore;

import com.amazon.titan.TestGraphUtil;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.LockKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

/**
*
* @author Alexander Patrikalakis
*
*/
public class AbstractDynamoDBLockStoreTest extends LockKeyColumnValueStoreTest {

    protected final BackendDataModel model;
    private String concreteClassName;
    protected AbstractDynamoDBLockStoreTest(BackendDataModel model) {
        //TODO(amcp) make this protected in super
        this.concreteClassName = getClass().getSimpleName();
        this.model = model;
    }

    @Override
    public DynamoDBStoreManager openStorageManager(int id /*ignore*/) throws BackendException {
        final List<String> storeNames = new ArrayList<String>(2);
        storeNames.add(DB_NAME);
        storeNames.add(DB_NAME + "_lock_");
        storeNames.add("multi_store_lock_0");
        storeNames.add("multi_store_lock_1");
        storeNames.add("multi_store_lock_2");
        storeNames.add("multi_store_lock_3");
        storeNames.add("multi_store_lock_4");
        storeNames.add("multi_store_lock_5");
        final WriteConfiguration wc = TestGraphUtil.instance().getStoreConfig(model, storeNames);
        final ModifiableConfiguration config = new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS, wc,
            BasicConfiguration.Restriction.NONE);
        //BEGIN LockKeyColumnValueStoreTest code L115
        config.set(GraphDatabaseConfiguration.LOCK_LOCAL_MEDIATOR_GROUP, concreteClassName + id);
        config.set(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID,"inst" + id);
        config.set(GraphDatabaseConfiguration.LOCK_RETRY,10);
        config.set(GraphDatabaseConfiguration.LOCK_EXPIRE, Duration.ofMillis(EXPIRE_MS));
        //END LockKeyColumnValueStoreTest code L118

        return new DynamoDBStoreManager(config);
    }

    @After
    public void cleanUpTables() throws Exception {
        TestGraphUtil.instance().cleanUpTables();
    }

    @Ignore
    @Override
    public void testRemoteLockContention() {
        //The DynamoDB Storage Backend for Titan does not support remote lock expiry currently.
        //Re-read the KeyColumns (edges, vertices, index entries) and retry.
        //If you enable a DynamoDB Stream on the store (table) and have a Lambda function
        //write the stream to a Kinesis stream, and then use KCL inside the AbstractDynamoDBStore,
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
    }
}
