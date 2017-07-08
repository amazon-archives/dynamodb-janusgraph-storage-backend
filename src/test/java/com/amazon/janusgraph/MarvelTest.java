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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Iterator;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.diskstorage.BackendException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.amazon.janusgraph.diskstorage.dynamodb.BackendDataModel;
import com.amazon.janusgraph.example.MarvelGraphFactory;
import com.amazon.janusgraph.graphdb.dynamodb.TestCombination;
import com.amazon.janusgraph.testcategory.IsolateRemainingTestsCategory;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;

/**
 *
 * @author Matthew Sowders
 * @author Alexander Patrikalakis
 */
@Category({IsolateRemainingTestsCategory.class})
@RunWith(Parameterized.class)
public class MarvelTest {

    private JanusGraph graph;

    @Before
    public void setUpGraph() throws Exception {
        graph = TestGraphUtil.instance.openGraph(model);
        MarvelTest.loadData(graph, 100 /* Number of lines to read from marvel.csv */);
    }

    @After
    public void tearDownGraph() throws BackendException {
        TestGraphUtil.instance.tearDownGraph(graph);
    }

    //TODO
    @Parameterized.Parameters//(name = "{0}")
    public static Collection<Object[]> data() {
        return TestCombination.NATIVE_LOCKING_CROSS_MODELS;
    }
    BackendDataModel model;
    public MarvelTest(final TestCombination combination) throws Exception {
        model = combination.getDataModel();
    }

    protected static void loadData(final JanusGraph graph, final int numLines) throws Exception {
        Preconditions.checkArgument(numLines >= 1, "Need to test with at least one line");
        // it takes a long time to process all 100,000 lines so we can
        // run a subset as a unit test.
        final int lines = Integer.valueOf(System.getProperty("MarvelTestLines", String.valueOf(numLines)));
        MarvelGraphFactory.load(graph, lines, false /*report*/);
    }

    @Test
    public void characterQuery() {
        final GraphTraversalSource g = graph.traversal();
        final Iterator<Vertex> it = g.V().has(MarvelGraphFactory.CHARACTER, "CAPTAIN AMERICA");
        assertTrue("Query should return a result", it.hasNext());
        final Vertex captainAmerica = it.next();
        assertNotNull("Query result should be non null", captainAmerica);
        assertNotNull("The properties should not be null", captainAmerica.property(MarvelGraphFactory.WEAPON));
    }

    @Test
    public void queryAllVertices() throws Exception {
        final Iterator<JanusGraphVertex> allVerticiesIterator = graph.query().vertices().iterator();
        final MetricRegistry registry = MarvelGraphFactory.REGISTRY;
        while (allVerticiesIterator.hasNext()) {
            final Vertex next = allVerticiesIterator.next();
            final String type;
            if (next.keys().contains(MarvelGraphFactory.COMIC_BOOK)) {
                // comic book
                type = MarvelGraphFactory.COMIC_BOOK;
            } else {
                type = MarvelGraphFactory.CHARACTER;
            }
            final Iterator<Edge> edges = next.edges(Direction.BOTH, MarvelGraphFactory.APPEARED);
            int edgeCount = 0;
            while (edges.hasNext()) {
                edges.next();
                edgeCount++;
            }
            registry.histogram("MarvelTest.testQuery.histogram.appeared." + type + ".degree").update(edgeCount);
        }
    }

}
