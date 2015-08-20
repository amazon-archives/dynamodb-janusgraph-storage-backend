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
import static org.junit.Assert.assertNull;

import java.util.Iterator;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

/**
 *
 * @author Matthew Sowders
 * @author Alexander Patrikalakis
 */
public abstract class AbstractGraphTestBase {
    public static final String TIME = "time";
    public static final String CONNECT_DESC = "connectDesc";
    private static TitanKey TIME_KEY;
    private static TitanLabel CONNECT_DESC_LABEL;

    protected static void createSchema(final TitanGraph graph) {
        TIME_KEY = graph.makeKey(TIME).dataType(Integer.class).single().make();
        CONNECT_DESC_LABEL = graph.makeLabel(CONNECT_DESC).sortKey(TIME_KEY).sortOrder(Order.DESC).make();
        graph.commit();
    }

    @Rule
    public TestName name = new TestName();

    @Test
    public void testConflictingMutation() throws Exception
    {
        final TitanGraph graph = getGraph();
        String nextLabel = "next";
        graph.makeLabel(nextLabel).oneToMany().make();

        Vertex v1 = graph.addVertex(null);
        Vertex v2 = graph.addVertex(null);
        Edge e1 = graph.addEdge(null, v1, v2, nextLabel);
        Object e1Id = e1.getId();
        graph.commit();

        e1 = graph.getEdge(e1Id);
        assertNotNull("Expected edge 1 to exist", e1);

        graph.removeEdge(e1);
        Edge e2 = graph.addEdge(e1Id, v1, v2, nextLabel);
        Object e2Id = e2.getId();

        graph.commit();

        e1 = graph.getEdge(e1Id);
        e2 = graph.getEdge(e2Id);

        assertNull("Expected edge 1 to be deleted", e1);
        assertNotNull("Expected edge 2 to exist", e2);
    }

    @Test
    public void testMultiQuery() throws Exception
    {
        final TitanGraph graph = getGraph();
        TitanKey namePropertyKey = graph.makeKey("mq_property").dataType(String.class).make();

        TitanVertex v1 = (TitanVertex) graph.addVertex(null);
        v1.setProperty(namePropertyKey, "v1");
        TitanVertex v2 = (TitanVertex) graph.addVertex(null);
        v2.setProperty(namePropertyKey, "v2");

        graph.commit();

        // reloading vertices in the new transaction
        v1 = (TitanVertex) graph.getVertex(v1.getID());
        v2 = (TitanVertex) graph.getVertex(v2.getID());
        Map<TitanVertex, Iterable<TitanProperty>> mqResult = graph.multiQuery(v1, v2)
                                                                  .properties();
        assertPropertyEquals(mqResult.get(v1), namePropertyKey, "v1");
        assertPropertyEquals(mqResult.get(v2), namePropertyKey, "v2");
    }

    @Test
    public void queryOnSortedKeys() {
        final TitanGraph graph = getGraph();
        TitanTransaction tx = graph.newTransaction();

        TitanVertex v = tx.addVertex();
        TitanVertex u = tx.addVertex();
        tx.commit();

        v = (TitanVertex) graph.getVertex(v.getID());
        u = (TitanVertex) graph.getVertex(u.getID());

        int numEdges = 100;
        for (int i = 0; i < numEdges; i++) {
            TitanEdge edge = v.addEdge(CONNECT_DESC_LABEL, u);
            edge.setProperty(TIME, i);
        }

        graph.commit();
        assertEquals(10, Iterables.size(u.query().labels(CONNECT_DESC).limit(10).vertices()));
        assertEquals(numEdges, Iterables.size(u.query().labels(CONNECT_DESC).vertices()));
    }

    @SuppressWarnings("unchecked")
    <T> void assertPropertyEquals(Iterable<TitanProperty> properties, TitanKey key, T expected) {
        T actual = null;
        Iterator<TitanProperty> iterator = properties.iterator();
        while (iterator.hasNext() && actual == null) {
            TitanProperty property = iterator.next();
            if (property.getPropertyKey().equals(key)) {
                actual = (T) property.getValue();
            }
        }
        assertEquals("Incorrect" + key.getName() + "value", expected, actual);
    }

    protected abstract TitanGraph getGraph();
}
