/*
 * Copyright 2014-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazon.janusgraph.diskstorage.dynamodb;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.KeyColumn;
import org.janusgraph.diskstorage.KeyColumnValueStoreTest;
import org.janusgraph.diskstorage.KeyValueStoreUtil;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.testutil.RandomGenerator;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import com.amazon.janusgraph.TestGraphUtil;
import com.amazon.janusgraph.testutils.CiHeartbeat;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

/**
 *
 * @author Alexander Patrikalakis
 * @author Johan Jacobs
 *
 */
public abstract class AbstractDynamoDbStoreTest extends KeyColumnValueStoreTest
{
    @Rule
    public final TestName testName = new TestName();

    private final int NUM_COLUMNS = 50;
    private final CiHeartbeat ciHeartbeat;
    protected final BackendDataModel model;
    protected AbstractDynamoDbStoreTest(final BackendDataModel model) {
        this.model = model;
        this.ciHeartbeat = new CiHeartbeat();
    }
    @Override
    public KeyColumnValueStoreManager openStorageManager() throws BackendException
    {
        final List<String> storeNames = Collections.singletonList("testStore1");
        final WriteConfiguration wc = TestGraphUtil.instance.getStoreConfig(model, storeNames);

        if (name.getMethodName().equals("parallelScanTest")) {
            wc.set("storage.dynamodb." + Constants.DYNAMODB_ENABLE_PARALLEL_SCAN.getName(), "true");
        }
        final BasicConfiguration config = new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS, wc,
            BasicConfiguration.Restriction.NONE);

        return new DynamoDBStoreManager(config);
    }

    @Override
    public void testConcurrentGetSliceAndMutate() throws ExecutionException, InterruptedException, BackendException {
        //begin code from https://github.com/thinkaurelius/titan/blob/1.0.0/titan-test/src/main/java/com/thinkaurelius/titan/diskstorage/KeyColumnValueStoreTest.java#L695
        testConcurrentStoreOpsCustom(true, NUM_COLUMNS);
        //end code from https://github.com/thinkaurelius/titan/blob/1.0.0/titan-test/src/main/java/com/thinkaurelius/titan/diskstorage/KeyColumnValueStoreTest.java#L695
    }

    @Override
    public void testConcurrentGetSlice() throws ExecutionException, InterruptedException, BackendException {
        //begin code from https://github.com/thinkaurelius/titan/blob/1.0.0/titan-test/src/main/java/com/thinkaurelius/titan/diskstorage/KeyColumnValueStoreTest.java#L695
        testConcurrentStoreOpsCustom(false, NUM_COLUMNS);
        //end code from https://github.com/thinkaurelius/titan/blob/1.0.0/titan-test/src/main/java/com/thinkaurelius/titan/diskstorage/KeyColumnValueStoreTest.java#L695
    }

    //TODO once on JanusGraph, make the wrapped method in superclass protected and externalize load factor. Remove copied code
    //https://github.com/awslabs/dynamodb-titan-storage-backend/issues/160
    //begin code from https://github.com/thinkaurelius/titan/blob/1.0.0/titan-test/src/main/java/com/thinkaurelius/titan/diskstorage/KeyColumnValueStoreTest.java#L703
    void testConcurrentStoreOpsCustom(final boolean deletionEnabled, final int numColumns) throws BackendException, ExecutionException, InterruptedException {
        // Load data fixture
        final String[][] values = generateValues();
        loadValues(values);

        /*
         * Must reopen transaction prior to deletes.
         *
         * This is due to the tx timestamps semantics.  The timestamp is set once
         * during the lifetime of the transaction, and multiple calls to mutate will
         * use the same timestamp on each call.  This causes deletions and additions of the
         * same k-v coordinates made in the same tx to conflict.  On Cassandra, the
         * addition will win and the delete will appear to be dropped.
         *
         * The transaction open right now has already loaded the test fixtures, so any
         * attempt to delete some of the fixture will appear to fail if carried out in this
         * transaction.
         */
        tx.commit();
        tx = startTx();

        // Setup executor and runnables
        final int NUM_THREADS = 64;
        final ExecutorService es = Executors.newFixedThreadPool(NUM_THREADS);
        final List<Runnable> tasks = new ArrayList<>(NUM_THREADS);
        final int trials = 500; // TODO was 5000 - load factor - reduce by a factor of 10
        for (int i = 0; i < NUM_THREADS; i++) {
            final Set<KeyColumn> deleted = Sets.newHashSet();
            if (!deletionEnabled) {
                tasks.add(new ConcurrentRandomSliceReader(values, deleted, trials, numColumns));
            } else {
                tasks.add(new ConcurrentRandomSliceReader(values, deleted, i, trials, numColumns));
            }
        }
        final List<Future<?>> futures = new ArrayList<>(NUM_THREADS);

        // Execute
        for (Runnable r : tasks) {
            futures.add(es.submit(r));
        }

        // Block to completion (and propagate any ExecutionExceptions that fall out of get)
        int collected = 0;
        for (Future<?> f : futures) {
            f.get();
            collected++;
        }

        assertEquals(NUM_THREADS, collected);
    }

    private class ConcurrentRandomSliceReader implements Runnable {

        private final String[][] values;
        private final Set<KeyColumn> d;
        private final int startKey;
        private final int endKey;
        private final int trials;
        private final boolean deletionEnabled;
        private final int numColumns;

        public ConcurrentRandomSliceReader(final String[][] values, final Set<KeyColumn> deleted, final int trials, final int numColumns) {
            this.values = values;
            this.d = deleted;
            this.startKey = 0;
            this.endKey = values.length;
            this.deletionEnabled = false;
            this.trials = trials;
            this.numColumns = numColumns;
        }

        public ConcurrentRandomSliceReader(final String[][] values, final Set<KeyColumn> deleted, final int key, final int trials, final int numColumns) {
            this.values = values;
            this.d = deleted;
            this.startKey = key % values.length;
            this.endKey = startKey + 1;
            this.deletionEnabled = true;
            this.trials = trials;
            this.numColumns = numColumns;
        }

        @Override
        public void run() {
            for (int t = 0; t < trials; t++) {
                final int key = RandomGenerator.randomInt(startKey, endKey);
                int start = RandomGenerator.randomInt(0, numColumns);
                if (start == numColumns - 1) {
                    start = numColumns - 2;
                }
                final int end = RandomGenerator.randomInt(start + 1, numColumns);
                final int limit = RandomGenerator.randomInt(1, 30);
                try {
                    if (deletionEnabled) {
                        final int delCol = RandomGenerator.randomInt(start, end);
                        final ImmutableList<StaticBuffer> deletions = ImmutableList.of(KeyValueStoreUtil.getBuffer(delCol));
                        store.mutate(KeyValueStoreUtil.getBuffer(key), KeyColumnValueStore.NO_ADDITIONS, deletions, tx);
                        d.add(new KeyColumn(key, delCol));
                        tx.commit();
                        tx = startTx();
                    }
                    checkSlice(values, d, key, start, end, limit);
                    checkSlice(values, d, key, start, end, -1);
                } catch (BackendException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
    //end code from https://github.com/thinkaurelius/titan/blob/1.0.0/titan-test/src/main/java/com/thinkaurelius/titan/diskstorage/KeyColumnValueStoreTest.java#L807

    @Before
    public void setUpTest() throws Exception {
        this.ciHeartbeat.startHeartbeat(this.testName.getMethodName());

    }
    //end code from https://github.com/thinkaurelius/titan/blob/1.0.0/titan-test/src/main/java/com/thinkaurelius/titan/diskstorage/KeyColumnValueStoreTest.java#L807

    @After
    public void tearDownTest() throws Exception {
        TestGraphUtil.instance.cleanUpTables();
        this.ciHeartbeat.stopHeartbeat();
    }
}
