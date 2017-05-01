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

import java.util.Iterator;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.junit.Test;

/**
 *
 * @author Matthew Sowders
 * @author Alexander Patrikalakis
 *
 */
public abstract class AbstractGraphOfTheGodsTestBase {
    private final long edges;
    private final long vertices;

    public AbstractGraphOfTheGodsTestBase(long edges, long vertices) {
        this.edges = edges;
        this.vertices = vertices;
    }

    @Test
    public void testQueryByName() throws Exception {
        final JanusGraph graph = getGraph();
        
        Iterator<Vertex> results = graph.traversal().V().has("name", "jupiter");
        assertTrue("Query should return a result", results.hasNext());
        Vertex jupiter = results.next();
        assertNotNull("Query result should be non null", jupiter);
    }

    @Test
    public void testGremlinGetV() throws Exception {
        final JanusGraph graph = getGraph();
        GraphTraversal<Vertex, Vertex> traversal = graph.traversal().V(); //could also do .count();
        long count = 0;
        while(traversal.hasNext()) {
        	traversal.next();
        	count++;
        }
        assertEquals("Expected the correct number of vertices", vertices, count);
    }

    @Test
    public void testQueryAllVertices() throws Exception {
        final JanusGraph graph = getGraph();
        Iterator<Vertex> results = graph.vertices();

        int count = 0;
        while (results.hasNext()) {
            assertNotNull("Expected non-null vertex", results.next());
            count++;
        }

        assertEquals("Expected the correct number of vertices", vertices, count);
    }

    @Test
    public void testQueryAllEdges() throws Exception {
        final JanusGraph graph = getGraph();
        Iterator<Edge> results = graph.traversal().E();

        int count = 0;
        while (results.hasNext()) {
            assertNotNull("Expected non-null edge", results.next());
            count++;
        }

        assertEquals("Expected the correct number of edges", edges, count);
    }

    protected abstract JanusGraph getGraph();
}
