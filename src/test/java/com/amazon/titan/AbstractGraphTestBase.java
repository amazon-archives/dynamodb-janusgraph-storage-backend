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
import static org.junit.Assert.assertNull;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.schema.TitanManagement;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

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
        TitanManagement mgmt = graph.openManagement();
        TIME_KEY = mgmt.makePropertyKey(TIME).dataType(Integer.class).make();
        CONNECT_DESC_LABEL = mgmt.makeEdgeLabel(CONNECT_DESC).make();
        mgmt.buildEdgeIndex(CONNECT_DESC_LABEL, "connectTimeDesc", Direction.BOTH, Order.decr, TIME_KEY);
        mgmt.commit();
    }

    @Rule
    public TestName name = new TestName();

    @Test
    public void testConflictingMutation() throws Exception {
        final TitanGraph graph = getGraph();
        final String nextLabel = "next";
        graph.makeEdgeLabel(nextLabel).multiplicity(Multiplicity.MANY2ONE).make();

        Vertex v1 = graph.addVertex();
        Vertex v2 = graph.addVertex();
        Edge e1 = v1.addEdge(nextLabel, v2);
        Object e1Id = e1.id();
        graph.tx().commit();

        e1 = graph.edges(e1Id).next();
        assertNotNull("Expected edge 1 to exist", e1);

        e1.remove();
        Edge e2 = v1.addEdge(nextLabel, v2);
        Object e2Id = e2.id();

        graph.tx().commit();

        e1 = graph.edges(e1Id).next();
        e2 = graph.edges(e2Id).next();

        assertNull("Expected edge 1 to be deleted", e1);
        assertNotNull("Expected edge 2 to exist", e2);
    }

    protected abstract TitanGraph getGraph();
}
