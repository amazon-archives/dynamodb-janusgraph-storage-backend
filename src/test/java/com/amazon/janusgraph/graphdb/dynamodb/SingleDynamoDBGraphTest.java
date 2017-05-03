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
package com.amazon.janusgraph.graphdb.dynamodb;

import static org.apache.tinkerpop.gremlin.structure.Direction.BOTH;
import static org.apache.tinkerpop.gremlin.structure.Direction.IN;
import static org.apache.tinkerpop.gremlin.structure.Direction.OUT;
import static org.janusgraph.graphdb.internal.RelationCategory.EDGE;
import static org.janusgraph.testutil.JanusGraphAssert.assertCount;
import static org.janusgraph.testutil.JanusGraphAssert.size;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.VertexList;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.attribute.Contain;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.internal.Order;
import org.janusgraph.graphdb.types.StandardEdgeLabelMaker;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.amazon.janusgraph.diskstorage.dynamodb.BackendDataModel;
import com.amazon.janusgraph.testcategory.GraphSimpleLogTestCategory;
import com.amazon.janusgraph.testcategory.SingleDynamoDBGraphTestCategory;
import com.amazon.janusgraph.testcategory.SingleItemTestCategory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 *
 * @author Alexander Patrikalakis
 *
 */
public class SingleDynamoDBGraphTest extends AbstractDynamoDBGraphTest {
    public SingleDynamoDBGraphTest()
    {
        super(BackendDataModel.SINGLE);
    }

    @Override
    public WriteConfiguration getConfiguration() {
        final WriteConfiguration wc = super.getConfiguration();
        final String methodName = testName.getMethodName();
        if(methodName.contains("testEdgesExceedCacheSize")) {
            //default: 20000, testEdgesExceedCacheSize fails at 16459, passes at 16400
            //this is the maximum number of edges supported for a vertex with no vertex partitioning.
            wc.set("cache.tx-cache-size", 16400);
        }
        return wc;
    }

    @Test
    @Override
    @Category({ SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testVertexCentricQuery() {
        //BEGIN COPIED CODE
        //https://github.com/JanusGraph/janusgraph/blob/v0.1.0/janusgraph-test/src/main/java/org/janusgraph/graphdb/JanusGraphTest.java#L2533
        int noVertices = 1450;
        makeVertexIndexedUniqueKey("name", String.class);
        PropertyKey time = makeKey("time", Integer.class);
        PropertyKey weight = makeKey("weight", Double.class);
        PropertyKey number = makeKey("number", Long.class);

        ((StandardEdgeLabelMaker) mgmt.makeEdgeLabel("connect")).sortKey(time).make();
        ((StandardEdgeLabelMaker) mgmt.makeEdgeLabel("connectDesc")).sortKey(time).sortOrder(Order.DESC).make();
        ((StandardEdgeLabelMaker) mgmt.makeEdgeLabel("friend")).sortKey(weight, time).sortOrder(Order.ASC).signature(number).make();
        ((StandardEdgeLabelMaker) mgmt.makeEdgeLabel("friendDesc")).sortKey(weight, time).sortOrder(Order.DESC).signature(number).make();
        ((StandardEdgeLabelMaker) mgmt.makeEdgeLabel("knows")).sortKey(number, weight).make();
        mgmt.makeEdgeLabel("follows").make();
        finishSchema();

        JanusGraphVertex v = tx.addVertex("name", "v");
        JanusGraphVertex u = tx.addVertex("name", "u");
        assertEquals(0, (noVertices - 1) % 3);
        JanusGraphVertex[] vs = new JanusGraphVertex[noVertices];
        for (int i = 1; i < noVertices; i++) {
            vs[i] = tx.addVertex("name", "v" + i);
        }
        EdgeLabel[] labelsV = {tx.getEdgeLabel("connect"), tx.getEdgeLabel("friend"), tx.getEdgeLabel("knows")};
        EdgeLabel[] labelsU = {tx.getEdgeLabel("connectDesc"), tx.getEdgeLabel("friendDesc"), tx.getEdgeLabel("knows")};
        for (int i = 1; i < noVertices; i++) {
            for (JanusGraphVertex vertex : new JanusGraphVertex[]{v, u}) {
                for (Direction d : new Direction[]{OUT, IN}) {
                    EdgeLabel label = vertex == v ? labelsV[i % 3] : labelsU[i % 3];
                    JanusGraphEdge e = d == OUT ? vertex.addEdge(n(label), vs[i]) :
                        vs[i].addEdge(n(label), vertex);
                    e.property("time", i);
                    e.property("weight", i % 4 + 0.5);
                    e.property("name", "e" + i);
                    e.property("number", i % 5);
                }
            }
        }
        int edgesPerLabel = noVertices / 3;


        VertexList vl;
        Map<JanusGraphVertex, Iterable<JanusGraphEdge>> results;
        Map<JanusGraphVertex, Iterable<JanusGraphVertexProperty>> results2;
        JanusGraphVertex[] qvs;
        int lastTime;
        Iterator<? extends Edge> outer;

        clopen();

        long[] vidsubset = new long[31 - 3];
        for (int i = 0; i < vidsubset.length; i++) vidsubset[i] = vs[i + 3].longId();
        Arrays.sort(vidsubset);

        //##################################################
        //Queries from Cache
        //##################################################
        clopen();
        for (int i = 1; i < noVertices; i++) vs[i] = getV(tx, vs[i].longId());
        v = getV(tx, v.longId());
        u = getV(tx, u.longId());
        qvs = new JanusGraphVertex[]{vs[6], vs[9], vs[12], vs[15], vs[60]};

        //To trigger queries from cache (don't copy!!!)
        assertCount(2 * (noVertices - 1), v.query().direction(Direction.BOTH).edges());


        assertEquals(1, v.query().propertyCount());

        assertEquals(10, size(v.query().labels("connect").limit(10).vertices()));
        assertEquals(10, size(u.query().labels("connectDesc").limit(10).vertices()));
        assertEquals(10, size(v.query().labels("connect").has("time", Cmp.GREATER_THAN, 30).limit(10).vertices()));
        assertEquals(10, size(u.query().labels("connectDesc").has("time", Cmp.GREATER_THAN, 30).limit(10).vertices()));

        lastTime = 0;
        for (JanusGraphEdge e : (Iterable<JanusGraphEdge>) v.query().labels("connect").direction(OUT).limit(20).edges()) {
            int nowTime = e.value("time");
            assertTrue(lastTime + " vs. " + nowTime, lastTime <= nowTime);
            lastTime = nowTime;
        }
        lastTime = Integer.MAX_VALUE;
        for (Edge e : (Iterable<JanusGraphEdge>) u.query().labels("connectDesc").direction(OUT).limit(20).edges()) {
            int nowTime = e.value("time");
            assertTrue(lastTime + " vs. " + nowTime, lastTime >= nowTime);
            lastTime = nowTime;
        }
        assertEquals(10, size(v.query().labels("connect").direction(OUT).has("time", Cmp.GREATER_THAN, 60).limit(10).vertices()));
        assertEquals(10, size(u.query().labels("connectDesc").direction(OUT).has("time", Cmp.GREATER_THAN, 60).limit(10).vertices()));

        outer = v.query().labels("connect").direction(OUT).limit(20).edges().iterator();
        for (Edge e : (Iterable<JanusGraphEdge>) v.query().labels("connect").direction(OUT).limit(10).edges()) {
            assertEquals(e, outer.next());
        }

        evaluateQuery(v.query().labels("connect").direction(OUT).interval("time", 3, 31), EDGE, 10, 1, new boolean[]{true, true});
        evaluateQuery(v.query().labels("connect").direction(OUT).has("time", 15).has("weight", 3.5), EDGE, 1, 1, new boolean[]{false, true});
        evaluateQuery(u.query().labels("connectDesc").direction(OUT).interval("time", 3, 31), EDGE, 10, 1, new boolean[]{true, true});
        assertEquals(10, v.query().labels("connect").direction(IN).interval("time", 3, 31).edgeCount());
        assertEquals(10, u.query().labels("connectDesc").direction(IN).interval("time", 3, 31).edgeCount());
        assertEquals(0, v.query().labels("connect").direction(OUT).has("time", null).edgeCount());
        assertEquals(10, v.query().labels("connect").direction(OUT).interval("time", 3, 31).vertexIds().size());
        assertEquals(edgesPerLabel - 10, v.query().labels("connect").direction(OUT).has("time", Cmp.GREATER_THAN, 31).count());
        assertEquals(10, size(v.query().labels("connect").direction(OUT).interval("time", 3, 31).vertices()));
        assertEquals(3, v.query().labels("friend").direction(OUT).limit(3).count());
        evaluateQuery(v.query().labels("friend").direction(OUT).has("weight", 0.5).limit(3), EDGE, 3, 1, new boolean[]{true, true});
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 3, 33).has("weight", 0.5), EDGE, 3, 1, new boolean[]{true, true});
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 3, 33).has("weight", Contain.IN, ImmutableList.of(0.5)), EDGE, 3, 1, new boolean[]{true, true});
        evaluateQuery(v.query().labels("friend").direction(OUT).has("weight", Contain.IN, ImmutableList.of(0.5, 1.5, 2.5)).interval("time", 3, 33), EDGE, 7, 3, new boolean[]{true, true});

        //TODO when fixing jg code, multiply 1667 by noVertices / 10000.0 and round up to get 242
        // https://github.com/awslabs/dynamodb-titan-storage-backend/issues/160
        //line to fix:
        //https://github.com/JanusGraph/janusgraph/blob/v0.1.0/janusgraph-test/src/main/java/org/janusgraph/graphdb/JanusGraphTest.java#L2638
        int friendsWhoHaveOutEdgesWithWeightBetweenPointFiveAndOnePointFive = (int) Math.round(Math.ceil(1667 * noVertices / 10000.0));
        evaluateQuery(v.query().labels("friend").direction(OUT).has("weight", Contain.IN, ImmutableList.of(0.5, 1.5)), EDGE,
            friendsWhoHaveOutEdgesWithWeightBetweenPointFiveAndOnePointFive, 2, new boolean[]{true, true});
        assertEquals(3, u.query().labels("friendDesc").direction(OUT).interval("time", 3, 33).has("weight", 0.5).edgeCount());
        assertEquals(1, v.query().labels("friend").direction(OUT).has("weight", 0.5).interval("time", 4, 10).edgeCount());
        assertEquals(1, u.query().labels("friendDesc").direction(OUT).has("weight", 0.5).interval("time", 4, 10).edgeCount());
        assertEquals(3, v.query().labels("friend").direction(OUT).interval("time", 3, 33).has("weight", 0.5).edgeCount());
        assertEquals(4, v.query().labels("friend").direction(OUT).has("time", Cmp.LESS_THAN_EQUAL, 10).edgeCount());
        assertEquals(edgesPerLabel - 4, v.query().labels("friend").direction(OUT).has("time", Cmp.GREATER_THAN, 10).edgeCount());
        assertEquals(20, v.query().labels("friend", "connect").direction(OUT).interval("time", 3, 33).edgeCount());

        assertEquals((int) Math.ceil(edgesPerLabel / 5.0), v.query().labels("knows").direction(OUT).has("number", 0).edgeCount());
        assertEquals((int) Math.ceil(edgesPerLabel / 5.0), v.query().labels("knows").direction(OUT).has("number", 0).interval("weight", 0.0, 4.0).edgeCount());
        assertEquals((int) Math.ceil(edgesPerLabel / (5.0 * 2)), v.query().labels("knows").direction(OUT).has("number", 0).interval("weight", 0.0, 2.0).edgeCount());
        assertEquals((int) Math.floor(edgesPerLabel / (5.0 * 2)), v.query().labels("knows").direction(OUT).has("number", 0).interval("weight", 2.1, 4.0).edgeCount());
        assertEquals(20, size(v.query().labels("connect", "friend").direction(OUT).interval("time", 3, 33).vertices()));
        assertEquals(20, size(v.query().labels("connect", "friend").direction(OUT).interval("time", 3, 33).vertexIds()));
        assertEquals(30, v.query().labels("friend", "connect", "knows").direction(OUT).interval("time", 3, 33).edgeCount());
        assertEquals(noVertices - 2, v.query().labels("friend", "connect", "knows").direction(OUT).has("time", Cmp.NOT_EQUAL, 10).edgeCount());

        assertEquals(0, v.query().has("age", null).labels("undefined").direction(OUT).edgeCount());
        assertEquals(1, v.query().labels("connect").direction(OUT).adjacent(vs[6]).has("time", 6).edgeCount());
        assertEquals(1, v.query().labels("knows").direction(OUT).adjacent(vs[11]).edgeCount());
        assertEquals(1, v.query().labels("knows").direction(IN).adjacent(vs[11]).edgeCount());
        assertEquals(2, v.query().labels("knows").direction(BOTH).adjacent(vs[11]).edgeCount());
        assertEquals(1, v.query().labels("knows").direction(OUT).adjacent(vs[11]).has("weight", 3.5).edgeCount());
        assertEquals(2, v.query().labels("connect").adjacent(vs[6]).has("time", 6).edgeCount());
        assertEquals(0, v.query().labels("connect").adjacent(vs[8]).has("time", 8).edgeCount());

        assertEquals(edgesPerLabel, v.query().labels("connect").direction(OUT).edgeCount());
        assertEquals(edgesPerLabel, v.query().labels("connect").direction(IN).edgeCount());
        assertEquals(2 * edgesPerLabel, v.query().labels("connect").direction(BOTH).edgeCount());

        assertEquals(edgesPerLabel, v.query().labels("connect").has("undefined", null).direction(OUT).edgeCount());
        assertEquals(2 * (int) Math.ceil((noVertices - 1) / 4.0), size(v.query().labels("connect", "friend", "knows").has("weight", 1.5).vertexIds()));
        assertEquals(1, v.query().direction(IN).has("time", 1).edgeCount());
        assertEquals(10, v.query().direction(OUT).interval("time", 4, 14).edgeCount());
        assertEquals(9, v.query().direction(IN).interval("time", 4, 14).has("time", Cmp.NOT_EQUAL, 10).edgeCount());
        assertEquals(9, v.query().direction(OUT).interval("time", 4, 14).has("time", Cmp.NOT_EQUAL, 10).edgeCount());
        assertEquals(noVertices - 1, size(v.query().direction(OUT).vertices()));
        assertEquals(noVertices - 1, size(v.query().direction(IN).vertices()));
        for (Direction dir : new Direction[]{IN, OUT}) {
            vl = v.query().labels().direction(dir).interval("time", 3, 31).vertexIds();
            vl.sort();
            for (int i = 0; i < vl.size(); i++) assertEquals(vidsubset[i], vl.getID(i));
        }
        assertCount(2 * (noVertices - 1), v.query().direction(Direction.BOTH).edges());


        //Property queries
        assertEquals(1, size(v.query().properties()));
        assertEquals(1, size(v.query().keys("name").properties()));

        //MultiQueries
        results = tx.multiQuery(qvs).direction(IN).labels("connect").edges();
        for (Iterable<JanusGraphEdge> result : results.values()) assertEquals(1, size(result));
        results = tx.multiQuery(Sets.newHashSet(qvs)).labels("connect").edges();
        for (Iterable<JanusGraphEdge> result : results.values()) assertEquals(2, size(result));
        results = tx.multiQuery(qvs).labels("knows").edges();
        for (Iterable<JanusGraphEdge> result : results.values()) assertEquals(0, size(result));
        results = tx.multiQuery(qvs).edges();
        for (Iterable<JanusGraphEdge> result : results.values()) assertEquals(4, size(result));
        results2 = tx.multiQuery(qvs).properties();
        for (Iterable<JanusGraphVertexProperty> result : results2.values()) assertEquals(1, size(result));
        results2 = tx.multiQuery(qvs).keys("name").properties();
        for (Iterable<JanusGraphVertexProperty> result : results2.values()) assertEquals(1, size(result));

        //##################################################
        //Same queries as above but without memory loading (i.e. omitting the first query)
        //##################################################
        clopen();
        for (int i = 1; i < noVertices; i++) vs[i] = getV(tx, vs[i].longId());
        v = getV(tx, v.longId());
        u = getV(tx, u.longId());
        qvs = new JanusGraphVertex[]{vs[6], vs[9], vs[12], vs[15], vs[60]};

        assertEquals(10, size(v.query().labels("connect").limit(10).vertices()));
        assertEquals(10, size(u.query().labels("connectDesc").limit(10).vertices()));
        assertEquals(10, size(v.query().labels("connect").has("time", Cmp.GREATER_THAN, 30).limit(10).vertices()));
        assertEquals(10, size(u.query().labels("connectDesc").has("time", Cmp.GREATER_THAN, 30).limit(10).vertices()));

        lastTime = 0;
        for (Edge e : (Iterable<JanusGraphEdge>) v.query().labels("connect").direction(OUT).limit(20).edges()) {
            int nowTime = e.value("time");
            assertTrue(lastTime + " vs. " + nowTime, lastTime <= nowTime);
            lastTime = nowTime;
        }
        lastTime = Integer.MAX_VALUE;
        for (Edge e : (Iterable<JanusGraphEdge>) u.query().labels("connectDesc").direction(OUT).limit(20).edges()) {
            int nowTime = e.value("time");
            assertTrue(lastTime + " vs. " + nowTime, lastTime >= nowTime);
            lastTime = nowTime;
        }
        assertEquals(10, size(v.query().labels("connect").direction(OUT).has("time", Cmp.GREATER_THAN, 60).limit(10).vertices()));
        assertEquals(10, size(u.query().labels("connectDesc").direction(OUT).has("time", Cmp.GREATER_THAN, 60).limit(10).vertices()));

        outer = v.query().labels("connect").direction(OUT).limit(20).edges().iterator();
        for (Edge e : (Iterable<JanusGraphEdge>) v.query().labels("connect").direction(OUT).limit(10).edges()) {
            assertEquals(e, outer.next());
        }

        evaluateQuery(v.query().labels("connect").direction(OUT).interval("time", 3, 31), EDGE, 10, 1, new boolean[]{true, true});
        evaluateQuery(v.query().labels("connect").direction(OUT).has("time", 15).has("weight", 3.5), EDGE, 1, 1, new boolean[]{false, true});
        evaluateQuery(u.query().labels("connectDesc").direction(OUT).interval("time", 3, 31), EDGE, 10, 1, new boolean[]{true, true});
        assertEquals(10, v.query().labels("connect").direction(IN).interval("time", 3, 31).edgeCount());
        assertEquals(10, u.query().labels("connectDesc").direction(IN).interval("time", 3, 31).edgeCount());
        assertEquals(0, v.query().labels("connect").direction(OUT).has("time", null).edgeCount());
        assertEquals(10, v.query().labels("connect").direction(OUT).interval("time", 3, 31).vertexIds().size());
        assertEquals(edgesPerLabel - 10, v.query().labels("connect").direction(OUT).has("time", Cmp.GREATER_THAN, 31).count());
        assertEquals(10, size(v.query().labels("connect").direction(OUT).interval("time", 3, 31).vertices()));
        assertEquals(3, v.query().labels("friend").direction(OUT).limit(3).count());
        evaluateQuery(v.query().labels("friend").direction(OUT).has("weight", 0.5).limit(3), EDGE, 3, 1, new boolean[]{true, true});
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 3, 33).has("weight", 0.5), EDGE, 3, 1, new boolean[]{true, true});
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 3, 33).has("weight", Contain.IN, ImmutableList.of(0.5)), EDGE, 3, 1, new boolean[]{true, true});
        evaluateQuery(v.query().labels("friend").direction(OUT).has("weight", Contain.IN, ImmutableList.of(0.5, 1.5, 2.5)).interval("time", 3, 33), EDGE, 7, 3, new boolean[]{true, true});
        //TODO one more change needed here.
        //https://github.com/awslabs/dynamodb-titan-storage-backend/issues/160
        evaluateQuery(v.query().labels("friend").direction(OUT).has("weight", Contain.IN, ImmutableList.of(0.5, 1.5)), EDGE,
            friendsWhoHaveOutEdgesWithWeightBetweenPointFiveAndOnePointFive, 2, new boolean[]{true, true});
        assertEquals(3, u.query().labels("friendDesc").direction(OUT).interval("time", 3, 33).has("weight", 0.5).edgeCount());
        assertEquals(1, v.query().labels("friend").direction(OUT).has("weight", 0.5).interval("time", 4, 10).edgeCount());
        assertEquals(1, u.query().labels("friendDesc").direction(OUT).has("weight", 0.5).interval("time", 4, 10).edgeCount());
        assertEquals(3, v.query().labels("friend").direction(OUT).interval("time", 3, 33).has("weight", 0.5).edgeCount());
        assertEquals(4, v.query().labels("friend").direction(OUT).has("time", Cmp.LESS_THAN_EQUAL, 10).edgeCount());
        assertEquals(edgesPerLabel - 4, v.query().labels("friend").direction(OUT).has("time", Cmp.GREATER_THAN, 10).edgeCount());
        assertEquals(20, v.query().labels("friend", "connect").direction(OUT).interval("time", 3, 33).edgeCount());

        assertEquals((int) Math.ceil(edgesPerLabel / 5.0), v.query().labels("knows").direction(OUT).has("number", 0).edgeCount());
        assertEquals((int) Math.ceil(edgesPerLabel / 5.0), v.query().labels("knows").direction(OUT).has("number", 0).interval("weight", 0.0, 4.0).edgeCount());
        assertEquals((int) Math.ceil(edgesPerLabel / (5.0 * 2)), v.query().labels("knows").direction(OUT).has("number", 0).interval("weight", 0.0, 2.0).edgeCount());
        assertEquals((int) Math.floor(edgesPerLabel / (5.0 * 2)), v.query().labels("knows").direction(OUT).has("number", 0).interval("weight", 2.1, 4.0).edgeCount());
        assertEquals(20, size(v.query().labels("connect", "friend").direction(OUT).interval("time", 3, 33).vertices()));
        assertEquals(20, size(v.query().labels("connect", "friend").direction(OUT).interval("time", 3, 33).vertexIds()));
        assertEquals(30, v.query().labels("friend", "connect", "knows").direction(OUT).interval("time", 3, 33).edgeCount());
        assertEquals(noVertices - 2, v.query().labels("friend", "connect", "knows").direction(OUT).has("time", Cmp.NOT_EQUAL, 10).edgeCount());

        assertEquals(0, v.query().has("age", null).labels("undefined").direction(OUT).edgeCount());
        assertEquals(1, v.query().labels("connect").direction(OUT).adjacent(vs[6]).has("time", 6).edgeCount());
        assertEquals(1, v.query().labels("knows").direction(OUT).adjacent(vs[11]).edgeCount());
        assertEquals(1, v.query().labels("knows").direction(IN).adjacent(vs[11]).edgeCount());
        assertEquals(2, v.query().labels("knows").direction(BOTH).adjacent(vs[11]).edgeCount());
        assertEquals(1, v.query().labels("knows").direction(OUT).adjacent(vs[11]).has("weight", 3.5).edgeCount());
        assertEquals(2, v.query().labels("connect").adjacent(vs[6]).has("time", 6).edgeCount());
        assertEquals(0, v.query().labels("connect").adjacent(vs[8]).has("time", 8).edgeCount());

        assertEquals(edgesPerLabel, v.query().labels("connect").direction(OUT).edgeCount());
        assertEquals(edgesPerLabel, v.query().labels("connect").direction(IN).edgeCount());
        assertEquals(2 * edgesPerLabel, v.query().labels("connect").direction(BOTH).edgeCount());

        assertEquals(edgesPerLabel, v.query().labels("connect").has("undefined", null).direction(OUT).edgeCount());
        assertEquals(2 * (int) Math.ceil((noVertices - 1) / 4.0), size(v.query().labels("connect", "friend", "knows").has("weight", 1.5).vertexIds()));
        assertEquals(1, v.query().direction(IN).has("time", 1).edgeCount());
        assertEquals(10, v.query().direction(OUT).interval("time", 4, 14).edgeCount());
        assertEquals(9, v.query().direction(IN).interval("time", 4, 14).has("time", Cmp.NOT_EQUAL, 10).edgeCount());
        assertEquals(9, v.query().direction(OUT).interval("time", 4, 14).has("time", Cmp.NOT_EQUAL, 10).edgeCount());
        assertEquals(noVertices - 1, size(v.query().direction(OUT).vertices()));
        assertEquals(noVertices - 1, size(v.query().direction(IN).vertices()));
        for (Direction dir : new Direction[]{IN, OUT}) {
            vl = v.query().labels().direction(dir).interval("time", 3, 31).vertexIds();
            vl.sort();
            for (int i = 0; i < vl.size(); i++) assertEquals(vidsubset[i], vl.getID(i));
        }
        assertCount(2 * (noVertices - 1), v.query().direction(Direction.BOTH).edges());


        //Property queries
        assertEquals(1, size(v.query().properties()));
        assertEquals(1, size(v.query().keys("name").properties()));

        //MultiQueries
        results = tx.multiQuery(qvs).direction(IN).labels("connect").edges();
        for (Iterable<JanusGraphEdge> result : results.values()) assertEquals(1, size(result));
        results = tx.multiQuery(Sets.newHashSet(qvs)).labels("connect").edges();
        for (Iterable<JanusGraphEdge> result : results.values()) assertEquals(2, size(result));
        results = tx.multiQuery(qvs).labels("knows").edges();
        for (Iterable<JanusGraphEdge> result : results.values()) assertEquals(0, size(result));
        results = tx.multiQuery(qvs).edges();
        for (Iterable<JanusGraphEdge> result : results.values()) assertEquals(4, size(result));
        results2 = tx.multiQuery(qvs).properties();
        for (Iterable<JanusGraphVertexProperty> result : results2.values()) assertEquals(1, size(result));
        results2 = tx.multiQuery(qvs).keys("name").properties();
        for (Iterable<JanusGraphVertexProperty> result : results2.values()) assertEquals(1, size(result));

        //##################################################
        //End copied queries
        //##################################################

        newTx();

        v = Iterables.<JanusGraphVertex>getOnlyElement(tx.query().has("name", "v").vertices());
        assertNotNull(v);
        assertEquals(2, v.query().has("weight", 1.5).interval("time", 10, 30).limit(2).vertexIds().size());
        assertEquals(10, v.query().has("weight", 1.5).interval("time", 10, 30).vertexIds().size());

        newTx();

        v = Iterables.<JanusGraphVertex>getOnlyElement(tx.query().has("name", "v").vertices());
        assertNotNull(v);
        assertEquals(2, v.query().has("weight", 1.5).interval("time", 10, 30).limit(2).edgeCount());
        assertEquals(10, v.query().has("weight", 1.5).interval("time", 10, 30).edgeCount());


        newTx();
        //Test partially new vertex queries
        JanusGraphVertex[] qvs2 = new JanusGraphVertex[qvs.length + 2];
        qvs2[0] = tx.addVertex();
        for (int i = 0; i < qvs.length; i++) qvs2[i + 1] = getV(tx, qvs[i].longId());
        qvs2[qvs2.length - 1] = tx.addVertex();
        qvs2[0].addEdge("connect", qvs2[qvs2.length - 1]);
        qvs2[qvs2.length - 1].addEdge("connect", qvs2[0]);
        results = tx.multiQuery(qvs2).direction(IN).labels("connect").edges();
        for (Iterable<JanusGraphEdge> result : results.values()) assertEquals(1, size(result));
        //END COPIED CODE
        //https://github.com/JanusGraph/janusgraph/blob/v0.1.0/janusgraph-test/src/main/java/org/janusgraph/graphdb/JanusGraphTest.java#L2844
    }


    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testOpenClose() {
        super.testOpenClose();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testLargeJointIndexRetrieval() {
        super.testLargeJointIndexRetrieval();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testMediumCreateRetrieve() {
        super.testMediumCreateRetrieve();
    }

    @Test(expected = IllegalArgumentException.class)
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testSettingTTLOnUnsupportedType() throws Exception {
        super.testSettingTTLOnUnsupportedType();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testSchemaNameChange() {
        super.testSchemaNameChange();
    }

    @Test
    @Override
    @Category({GraphSimpleLogTestCategory.class, SingleItemTestCategory.class })
    public void simpleLogTest() throws InterruptedException {
        super.simpleLogTest();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testSchemaTypes() {
        super.testSchemaTypes();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testTinkerPopOptimizationStrategies() {
        super.testTinkerPopOptimizationStrategies();
    }

    @Test(expected = IllegalArgumentException.class)
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testGetTTLFromUnsupportedType() throws Exception {
        super.testGetTTLFromUnsupportedType();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testLocalGraphConfiguration() {
        super.testLocalGraphConfiguration();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testConcurrentConsistencyEnforcement() throws Exception {
        super.testConcurrentConsistencyEnforcement();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testTransactionalScopeOfSchemaTypes() {
        super.testTransactionalScopeOfSchemaTypes();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testNestedTransactions() {
        super.testNestedTransactions();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testBasic() {
        super.testBasic();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testUnsettingTTL() throws InterruptedException {
        super.testUnsettingTTL();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testGlobalOfflineGraphConfig() {
        super.testGlobalOfflineGraphConfig();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testLimitWithMixedIndexCoverage() {
        super.testLimitWithMixedIndexCoverage();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testMultivaluedVertexProperty() {
        super.testMultivaluedVertexProperty();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testGlobalGraphConfig() {
        super.testGlobalGraphConfig();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testManagedOptionMasking() throws BackendException {
        super.testManagedOptionMasking();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testGlobalGraphIndexingAndQueriesForInternalIndexes() {
        super.testGlobalGraphIndexingAndQueriesForInternalIndexes();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testWithoutIndex() {
        super.testWithoutIndex();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testIndexUpdatesWithReindexAndRemove() throws InterruptedException, ExecutionException {
        super.testIndexUpdatesWithReindexAndRemove();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testEdgeTTLTiming() throws Exception {
        super.testEdgeTTLTiming();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testStaleVertex() {
        super.testStaleVertex();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testGettingUndefinedVertexLabelTTL() {
        super.testGettingUndefinedVertexLabelTTL();
    }

    @Test
    @Override
    @Category({GraphSimpleLogTestCategory.class, SingleItemTestCategory.class })
    public void simpleLogTestWithFailure() throws InterruptedException {
        super.simpleLogTestWithFailure();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testVertexCentricIndexWithNull() {
        super.testVertexCentricIndexWithNull();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testVertexTTLImplicitKey() throws Exception {
        super.testVertexTTLImplicitKey();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testImplicitKey() {
        super.testImplicitKey();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testMaskableGraphConfig() {
        super.testMaskableGraphConfig();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testDataTypes() throws Exception {
        super.testDataTypes();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testEdgeTTLImplicitKey() throws Exception {
        super.testEdgeTTLImplicitKey();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testTinkerPopCardinality() {
        super.testTinkerPopCardinality();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testPropertyCardinality() {
        super.testPropertyCardinality();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testArrayEqualityUsingImplicitKey() {
        super.testArrayEqualityUsingImplicitKey();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testFixedGraphConfig() {
        super.testFixedGraphConfig();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testAutomaticTypeCreation() {
        super.testAutomaticTypeCreation();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testGettingUndefinedEdgeLabelTTL() {
        super.testGettingUndefinedEdgeLabelTTL();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testSimpleTinkerPopTraversal() {
        super.testSimpleTinkerPopTraversal();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testGlobalIteration() {
        super.testGlobalIteration();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testVertexRemoval() {
        super.testVertexRemoval();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testForceIndexUsage() {
        super.testForceIndexUsage();
    }

    @Test(expected = IllegalArgumentException.class)
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testSettingTTLOnNonStaticVertexLabel() throws Exception {
        super.testSettingTTLOnNonStaticVertexLabel();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testTransactionConfiguration() {
        super.testTransactionConfiguration();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testConsistencyEnforcement() {
        super.testConsistencyEnforcement();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testHasNot() {
        super.testHasNot();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testVertexTTLWithCompositeIndex() throws Exception {
        super.testVertexTTLWithCompositeIndex();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testRelationTypeIndexes() {
        super.testRelationTypeIndexes();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testGotGIndexRemoval() throws InterruptedException, ExecutionException {
        super.testGotGIndexRemoval();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testTransactionIsolation() {
        super.testTransactionIsolation();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testSelfLoop() {
        super.testSelfLoop();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testIndexUniqueness() {
        super.testIndexUniqueness();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testEdgeTTLWithTransactions() throws Exception {
        super.testEdgeTTLWithTransactions();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testIndexQueryWithLabelsAndContainsIN() {
        super.testIndexQueryWithLabelsAndContainsIN();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testEdgesExceedCacheSize() {
        super.testEdgesExceedCacheSize();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testThreadBoundTx() {
        super.testThreadBoundTx();
    }

    @Test
    @Override
    @Category({SingleDynamoDBGraphTestCategory.class, SingleItemTestCategory.class })
    public void testCreateDelete() {
        super.testCreateDelete();
    }

}
