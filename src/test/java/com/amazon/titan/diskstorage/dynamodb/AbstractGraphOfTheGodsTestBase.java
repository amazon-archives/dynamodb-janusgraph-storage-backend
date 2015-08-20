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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.junit.Test;

import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.java.GremlinPipeline;

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
        final TitanGraph graph = getGraph();
        Iterable<Vertex> results = graph.getVertices("name", "jupiter");
        assertTrue("Query should return a result", results.iterator().hasNext());
        Vertex jupiter = results.iterator().next();
        assertNotNull("Query result should be non null", jupiter);
    }

    @Test
    public void testGremlinGetV() throws Exception {
        final TitanGraph graph = getGraph();
        GremlinPipeline<Graph, Vertex> pipe = new GremlinPipeline<Graph, Vertex>();
        pipe = pipe.V();
        int count = 0;
        pipe.start(graph);
        while (pipe.hasNext()) {
            pipe.next();
            count++;
        }
        assertEquals("Expected the correct number of vertices", vertices, count);
    }

    @Test
    public void testQueryAllVertices() throws Exception {
        final TitanGraph graph = getGraph();
        Iterator<Vertex> results = graph.getVertices().iterator();

        int count = 0;
        while (results.hasNext()) {
            assertNotNull("Expected non-null vertex", results.next());
            count++;
        }

        assertEquals("Expected the correct number of vertices", vertices, count);
    }

    @Test
    public void testQueryAllEdges() throws Exception {
        final TitanGraph graph = getGraph();
        Iterator<Edge> results = graph.getEdges().iterator();

        int count = 0;
        while (results.hasNext()) {
            assertNotNull("Expected non-null edge", results.next());
            count++;
        }

        assertEquals("Expected the correct number of edges", edges, count);
    }

    protected abstract TitanGraph getGraph();
}
