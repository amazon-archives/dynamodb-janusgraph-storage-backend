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
package com.amazon.janusgraph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Iterator;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.example.GraphOfTheGodsFactory;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.amazon.janusgraph.graphdb.dynamodb.TestCombination;
import com.amazon.janusgraph.testcategory.IsolateRemainingTestsCategory;

/**
 *
 * @author Matthew Sowders
 * @author Alexander Patrikalakis
 *
 */
@Category({IsolateRemainingTestsCategory.class})
@RunWith(Parameterized.class)
public class GraphOfTheGodsTest {
    private static final long EDGES = 17;
    private static final long VERTICES = 12;

    private final JanusGraph graph;

    //TODO
    @Parameterized.Parameters//(name = "{0}")
    public static Collection<Object[]> data() {
        return TestCombination.NATIVE_LOCKING_CROSS_MODELS;
    }
    public GraphOfTheGodsTest(final TestCombination combination) {
        graph = TestGraphUtil.instance.openGraph(combination.getDataModel());
        GraphOfTheGodsFactory.loadWithoutMixedIndex(graph, true);
    }

    @After
    public void tearDownGraph() throws BackendException {
        TestGraphUtil.instance.tearDownGraph(graph);
    }

    @Test
    public void testQueryByName() throws Exception {
        final Iterator<Vertex> results = graph.traversal().V().has("name", "jupiter");
        assertTrue("Query should return a result", results.hasNext());
        final Vertex jupiter = results.next();
        assertNotNull("Query result should be non null", jupiter);
    }

    @Test
    public void testQueryAllVertices() throws Exception {
        assertEquals("Expected the correct number of VERTICES",
            VERTICES, graph.traversal().V().count().tryNext().get().longValue());
    }

    @Test
    public void testQueryAllEdges() throws Exception {
        assertEquals("Expected the correct number of EDGES",
            EDGES, graph.traversal().E().count().tryNext().get().longValue());
    }
}
