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
package com.amazon.titan;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.junit.Test;

import com.amazon.titan.example.MarvelGraphFactory;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 *
 * @author Matthew Sowders
 */
public abstract class AbstractMarvelTest {

    protected static void loadData(TitanGraph graph, int numLines) throws Exception {
        Preconditions.checkArgument(numLines >= 1, "Need to test with at least one line");
        // it takes a long time to process all 100,000 lines so we can
        // run a subset as a unit test.
        int lines = Integer.valueOf(System.getProperty("MarvelTestLines", String.valueOf(numLines)));
        MarvelGraphFactory.load(graph, lines, false /*report*/);
    }

    @Test
    public void characterQuery() {
        final TitanGraph graph = getGraph();
        final GraphTraversalSource g = graph.traversal();
        final Iterator<Vertex> it = g.V().has(MarvelGraphFactory.CHARACTER, "CAPTAIN AMERICA");
        assertTrue("Query should return a result", it.hasNext());
        Vertex captainAmerica = it.next();
        assertNotNull("Query result should be non null", captainAmerica);
        assertNotNull("The properties should not be null", captainAmerica.property(MarvelGraphFactory.WEAPON));
    }

    @Test
    public void queryAllVertices() throws Exception {
        final TitanGraph graph = getGraph();

        Iterator<TitanVertex> allVerticiesIterator = graph.query().vertices().iterator();
        MetricRegistry registry = MarvelGraphFactory.REGISTRY;
        while (allVerticiesIterator.hasNext()) {
            Vertex next = allVerticiesIterator.next();
            String type;
            if (next.keys().contains(MarvelGraphFactory.COMIC_BOOK)) {
                // comic book
                type = MarvelGraphFactory.COMIC_BOOK;
            } else {
                type = MarvelGraphFactory.CHARACTER;
            }
            Iterator<Edge> edges = next.edges(Direction.BOTH, MarvelGraphFactory.APPEARED);
            int edgeCount = 0;
            while (edges.hasNext()) {
                edges.next();
                edgeCount++;
            }
            registry.histogram("MarvelTest.testQuery.histogram.appeared." + type + ".degree").update(edgeCount);
        }
    }

    protected abstract TitanGraph getGraph();

}
