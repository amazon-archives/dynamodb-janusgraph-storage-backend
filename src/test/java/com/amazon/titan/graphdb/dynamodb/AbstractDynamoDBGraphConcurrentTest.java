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
package com.amazon.titan.graphdb.dynamodb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.amazon.titan.testutils.TravisCiHeartbeat;
import org.junit.*;

import com.amazon.titan.TestGraphUtil;
import com.amazon.titan.diskstorage.dynamodb.BackendDataModel;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.TitanGraphConcurrentTest;
import org.junit.rules.TestName;

/**
 *
 * @author Alexander Patrikalakis
 *
 */
public abstract class AbstractDynamoDBGraphConcurrentTest extends TitanGraphConcurrentTest
{

    @Rule
    public TestName testName = new TestName();

    private TravisCiHeartbeat travisCiHeartbeat;
    protected final BackendDataModel model;
    protected AbstractDynamoDBGraphConcurrentTest(BackendDataModel model) {
        this.model = model;
        this.travisCiHeartbeat = new TravisCiHeartbeat();
    }

    @Override
    public WriteConfiguration getConfiguration()
    {
        return TestGraphUtil.instance().graphConfig(model);
    }

    @AfterClass
    public static void deleteTables() throws BackendException {
        TestGraphUtil.instance().cleanUpTables();
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();

        this.travisCiHeartbeat.startHeartbeat(this.testName.getMethodName());
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();

        this.travisCiHeartbeat.stopHeartBeat();
    }

    //begin titan-test code - modified because test takes too long, kept number of runnables
    //the same
    //https://github.com/thinkaurelius/titan/blob/0.9.0-M2/titan-test/src/main/java/com/thinkaurelius/titan/graphdb/TitanGraphConcurrentTest.java#L291
    @Test
    @Override
    public void testStandardIndexVertexPropertyReads() throws InterruptedException, ExecutionException {
        final int propCount = 20; //THREAD_COUNT * 5;
        final int vertexCount = 1 * 1000;
        // Create props with standard indexes
        for (int i = 0; i < propCount; i++) {
            makeVertexIndexedUniqueKey("p"+i,String.class);
        }
        finishSchema();

        // Write vertices with indexed properties
        for (int i = 0; i < vertexCount; i++) {
            TitanVertex v = tx.addVertex();
            for (int p = 0; p < propCount; p++) {
                v.property("p" + p, i);
            }
        }
        newTx();
        // Execute runnables
        final int taskCount = 4 * 256;
        final ExecutorService executor = Executors.newFixedThreadPool(256);
        Collection<Future<?>> futures = new ArrayList<Future<?>>(taskCount);
        for (int i = 0; i < taskCount; i++) {
            futures.add(executor.submit(new VertexPropertyQuerier(propCount, vertexCount)));
        }
        for (Future<?> f : futures) {
            f.get();
        }
    }
    //https://github.com/thinkaurelius/titan/blob/0.9.0-M2/titan-test/src/main/java/com/thinkaurelius/titan/graphdb/TitanGraphConcurrentTest.java#L320

    //begin titan-test code - this was private so needed to copy
    //https://github.com/thinkaurelius/titan/blob/0.9.0-M2/titan-test/src/main/java/com/thinkaurelius/titan/graphdb/TitanGraphConcurrentTest.java#L456
    private class VertexPropertyQuerier implements Runnable {

        private final int propCount;
        private final int vertexCount;

        public VertexPropertyQuerier(int propCount, int vertexCount) {
            this.propCount = propCount;
            this.vertexCount = vertexCount;
        }

        @Override
        public void run() {
            for (int i = 0; i < vertexCount; i++) {
                for (int p = 0; p < propCount; p++) {
                    Iterables.size(tx.query().has("p" + p, i).vertices());
                }
            }
        }
    }
    //https://github.com/thinkaurelius/titan/blob/0.9.0-M2/titan-test/src/main/java/com/thinkaurelius/titan/graphdb/TitanGraphConcurrentTest.java#L474
}
