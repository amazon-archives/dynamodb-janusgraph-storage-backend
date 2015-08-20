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
import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.tinkerpop.blueprints.Direction;
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
    private static PropertyKey TIME_KEY;
    private static EdgeLabel CONNECT_DESC_LABEL;

    protected static void createSchema(final TitanGraph graph) {
        TitanManagement mgmt = graph.getManagementSystem();
        TIME_KEY = mgmt.makePropertyKey(TIME).dataType(Integer.class).make();
        CONNECT_DESC_LABEL = mgmt.makeEdgeLabel(CONNECT_DESC).make();
        mgmt.buildEdgeIndex(CONNECT_DESC_LABEL, "connectTimeDesc", Direction.BOTH, Order.DESC, TIME_KEY);
        mgmt.commit();
    }

    @Rule
    public TestName name = new TestName();

    @Test
    public void testConflictingMutation() throws Exception {
        final TitanGraph graph = getGraph();
        String nextLabel = "next";
        graph.makeEdgeLabel(nextLabel).multiplicity(Multiplicity.MANY2ONE).make();

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
    public void testMultiQuery() throws Exception {
        final TitanGraph graph = getGraph();
        PropertyKey namePropertyKey = graph.makePropertyKey("mq_property").dataType(String.class).make();

        TitanVertex v1 = graph.addVertex();
        v1.setProperty(namePropertyKey, "v1");
        TitanVertex v2 = graph.addVertex();
        v2.setProperty(namePropertyKey, "v2");

        graph.commit();

        // reloading vertices in the new transaction
        v1 = graph.getVertex(v1.getLongId());
        v2 = graph.getVertex(v2.getLongId());
        Map<TitanVertex, Iterable<TitanProperty>> mqResult = graph.multiQuery(v1, v2)
                                                                  .properties();
        assertPropertyEquals(mqResult.get(v1), namePropertyKey, "v1");
        assertPropertyEquals(mqResult.get(v2), namePropertyKey, "v2");
    }

    @Test
    public void queryOnSortedKeys() {
        final TitanGraph graph = getGraph();

        TitanVertex v = graph.addVertex();
        TitanVertex u = graph.addVertex();
        graph.commit();

        v = graph.getVertex(v.getLongId());
        u = graph.getVertex(u.getLongId());

        int numEdges = 100;
        for (int i = 0; i < numEdges; i++) {
            TitanEdge edge = v.addEdge(CONNECT_DESC, u);
            edge.setProperty(TIME, i);
        }

        graph.commit();
        assertEquals(10, Iterables.size(u.query().labels(CONNECT_DESC).limit(10).vertices()));
        assertEquals(numEdges, Iterables.size(u.query().labels(CONNECT_DESC).vertices()));
    }

    private <T> void assertPropertyEquals(Iterable<TitanProperty> properties, PropertyKey key, T expected) {
        T actual = null;
        Iterator<TitanProperty> iterator = properties.iterator();
        while (iterator.hasNext() && actual == null) {
            TitanProperty property = iterator.next();
            if (property.getPropertyKey().equals(key)) {
                actual = property.getValue();
            }
        }
        assertEquals("Incorrect" + key.getName() + "value", expected, actual);
    }

    protected abstract TitanGraph getGraph();
}
