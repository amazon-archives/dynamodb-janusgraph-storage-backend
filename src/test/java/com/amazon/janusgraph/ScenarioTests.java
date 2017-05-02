/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazon.janusgraph;

import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.diskstorage.BackendException;
import org.junit.Test;

import com.amazon.janusgraph.diskstorage.dynamodb.BackendDataModel;
import com.google.common.base.Stopwatch;

/**
 *
 * @author Alexander Patrikalakis
 *
 */
public class ScenarioTests {

    public static final String LABEL = "myLabel";

    /**
     * This test is to demonstrate performance in response to a report of elevated latency for committing 30 vertices.
     * http://stackoverflow.com/questions/42899388/titan-dynamodb-local-incredibly-slow-8s-commit-for-30-vertices
     * @throws BackendException
     */
    @Test
    public void performanceTest() throws BackendException {
        final Graph graph = JanusGraphFactory.open(TestGraphUtil.instance.createTestGraphConfig(BackendDataModel.MULTI));
        IntStream.of(30).forEach(i -> graph.addVertex(LABEL));
        Stopwatch watch = Stopwatch.createStarted();
        graph.tx().commit();
        System.out.println("Committing took " + watch.stop().elapsed(TimeUnit.MILLISECONDS) + " ms");
        TestGraphUtil.instance.cleanUpTables();
    }
}
