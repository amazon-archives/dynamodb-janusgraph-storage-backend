/*
 * Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Portions copyright Titan: Distributed Graph Database - Copyright 2012 and onwards Aurelius.
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
package com.amazon.titan.graphdb.dynamodb;

import static com.thinkaurelius.titan.graphdb.internal.RelationCategory.EDGE;
import static com.thinkaurelius.titan.testutil.TitanAssert.assertCount;
import static com.thinkaurelius.titan.testutil.TitanAssert.size;
import static org.apache.tinkerpop.gremlin.structure.Direction.BOTH;
import static org.apache.tinkerpop.gremlin.structure.Direction.IN;
import static org.apache.tinkerpop.gremlin.structure.Direction.OUT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import com.amazon.titan.testcategory.SingleDynamoDBGraphTestCategory;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.amazon.titan.diskstorage.dynamodb.BackendDataModel;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.TitanVertexProperty;
import com.thinkaurelius.titan.core.VertexList;
import com.thinkaurelius.titan.core.attribute.Cmp;
import com.thinkaurelius.titan.core.attribute.Contain;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.internal.Order;
import com.thinkaurelius.titan.graphdb.types.StandardEdgeLabelMaker;

/**
 *
 * @author Alexander Patrikalakis
 *
 */
@Category({ SingleDynamoDBGraphTestCategory.class })
public class SingleDynamoDBGraphTest extends AbstractDynamoDBGraphTest {
    public SingleDynamoDBGraphTest()
    {
        super(BackendDataModel.SINGLE);
    }

    @Override
    public WriteConfiguration getConfiguration() {
        final WriteConfiguration wc = super.getConfiguration();
        final String methodName = name.getMethodName();
        if(methodName.contains("testEdgesExceedCacheSize")) {
            //default: 20000, testEdgesExceedCacheSize fails at 16461, passes at 16460
            //this is the maximum number of edges supported for a vertex with no vertex partitioning.
            wc.set("cache.tx-cache-size", 16460);
        }
        return wc;
    }

    //begin titan-test code - modified to accommodate item size limit of SINGLE data model
    //https://github.com/thinkaurelius/titan/blob/0.5.4/titan-test/src/main/java/com/thinkaurelius/titan/graphdb/TitanGraphTest.java#L2481
    @Test @Override
    @SuppressWarnings("deprecation")
    public void testVertexCentricQuery() {
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

        TitanVertex v = tx.addVertex("name", "v");
        TitanVertex u = tx.addVertex("name", "u");
        int noVertices = 1450;
        assertEquals(0, (noVertices - 1) % 3);
        TitanVertex[] vs = new TitanVertex[noVertices];
        for (int i = 1; i < noVertices; i++) {
            vs[i] = tx.addVertex("name", "v" + i);
        }
        EdgeLabel[] labelsV = {tx.getEdgeLabel("connect"), tx.getEdgeLabel("friend"), tx.getEdgeLabel("knows")};
        EdgeLabel[] labelsU = {tx.getEdgeLabel("connectDesc"), tx.getEdgeLabel("friendDesc"), tx.getEdgeLabel("knows")};
        for (int i = 1; i < noVertices; i++) {
            for (TitanVertex vertex : new TitanVertex[]{v, u}) {
                for (Direction d : new Direction[]{OUT, IN}) {
                    EdgeLabel label = vertex == v ? labelsV[i % 3] : labelsU[i % 3];
                    TitanEdge e = d == OUT ? vertex.addEdge(n(label), vs[i]) :
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
        Map<TitanVertex, Iterable<TitanEdge>> results;
        Map<TitanVertex, Iterable<TitanVertexProperty>> results2;
        TitanVertex[] qvs;
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
        qvs = new TitanVertex[]{vs[6], vs[9], vs[12], vs[15], vs[60]};

        //To trigger queries from cache (don't copy!!!)
        assertCount(2 * (noVertices - 1), v.query().direction(Direction.BOTH).edges());


        assertEquals(1, v.query().propertyCount());

        assertEquals(10, size(v.query().labels("connect").limit(10).vertices()));
        assertEquals(10, size(u.query().labels("connectDesc").limit(10).vertices()));
        assertEquals(10, size(v.query().labels("connect").has("time", Cmp.GREATER_THAN, 30).limit(10).vertices()));
        assertEquals(10, size(u.query().labels("connectDesc").has("time", Cmp.GREATER_THAN, 30).limit(10).vertices()));

        lastTime = 0;
        for (TitanEdge e : (Iterable<TitanEdge>) v.query().labels("connect").direction(OUT).limit(20).edges()) {
            int nowTime = e.value("time");
            assertTrue(lastTime + " vs. " + nowTime, lastTime <= nowTime);
            lastTime = nowTime;
        }
        lastTime = Integer.MAX_VALUE;
        for (Edge e : (Iterable<TitanEdge>) u.query().labels("connectDesc").direction(OUT).limit(20).edges()) {
            int nowTime = e.value("time");
            assertTrue(lastTime + " vs. " + nowTime, lastTime >= nowTime);
            lastTime = nowTime;
        }
        assertEquals(10, size(v.query().labels("connect").direction(OUT).has("time", Cmp.GREATER_THAN, 60).limit(10).vertices()));
        assertEquals(10, size(u.query().labels("connectDesc").direction(OUT).has("time", Cmp.GREATER_THAN, 60).limit(10).vertices()));

        outer = v.query().labels("connect").direction(OUT).limit(20).edges().iterator();
        for (Edge e : (Iterable<TitanEdge>) v.query().labels("connect").direction(OUT).limit(10).edges()) {
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
        //after EDGE, was 1667
        evaluateQuery(v.query().labels("friend").direction(OUT).has("weight", Contain.IN, ImmutableList.of(0.5, 1.5)), EDGE, 242, 2, new boolean[]{true, true});
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
        for (Iterable<TitanEdge> result : results.values()) assertEquals(1, size(result));
        results = tx.multiQuery(Sets.newHashSet(qvs)).labels("connect").edges();
        for (Iterable<TitanEdge> result : results.values()) assertEquals(2, size(result));
        results = tx.multiQuery(qvs).labels("knows").edges();
        for (Iterable<TitanEdge> result : results.values()) assertEquals(0, size(result));
        results = tx.multiQuery(qvs).edges();
        for (Iterable<TitanEdge> result : results.values()) assertEquals(4, size(result));
        results2 = tx.multiQuery(qvs).properties();
        for (Iterable<TitanVertexProperty> result : results2.values()) assertEquals(1, size(result));
        results2 = tx.multiQuery(qvs).keys("name").properties();
        for (Iterable<TitanVertexProperty> result : results2.values()) assertEquals(1, size(result));

        //##################################################
        //Same queries as above but without memory loading (i.e. omitting the first query)
        //##################################################
        clopen();
        for (int i = 1; i < noVertices; i++) vs[i] = getV(tx, vs[i].longId());
        v = getV(tx, v.longId());
        u = getV(tx, u.longId());
        qvs = new TitanVertex[]{vs[6], vs[9], vs[12], vs[15], vs[60]};

        assertEquals(10, size(v.query().labels("connect").limit(10).vertices()));
        assertEquals(10, size(u.query().labels("connectDesc").limit(10).vertices()));
        assertEquals(10, size(v.query().labels("connect").has("time", Cmp.GREATER_THAN, 30).limit(10).vertices()));
        assertEquals(10, size(u.query().labels("connectDesc").has("time", Cmp.GREATER_THAN, 30).limit(10).vertices()));

        lastTime = 0;
        for (Edge e : (Iterable<TitanEdge>) v.query().labels("connect").direction(OUT).limit(20).edges()) {
            int nowTime = e.value("time");
            assertTrue(lastTime + " vs. " + nowTime, lastTime <= nowTime);
            lastTime = nowTime;
        }
        lastTime = Integer.MAX_VALUE;
        for (Edge e : (Iterable<TitanEdge>) u.query().labels("connectDesc").direction(OUT).limit(20).edges()) {
            int nowTime = e.value("time");
            assertTrue(lastTime + " vs. " + nowTime, lastTime >= nowTime);
            lastTime = nowTime;
        }
        assertEquals(10, size(v.query().labels("connect").direction(OUT).has("time", Cmp.GREATER_THAN, 60).limit(10).vertices()));
        assertEquals(10, size(u.query().labels("connectDesc").direction(OUT).has("time", Cmp.GREATER_THAN, 60).limit(10).vertices()));

        outer = v.query().labels("connect").direction(OUT).limit(20).edges().iterator();
        for (Edge e : (Iterable<TitanEdge>) v.query().labels("connect").direction(OUT).limit(10).edges()) {
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
        evaluateQuery(v.query().labels("friend").direction(OUT).has("weight", Contain.IN, ImmutableList.of(0.5, 1.5)), EDGE, 242 /*1667*/, 2, new boolean[]{true, true});
        //change the line from 1667 to 242 (approximately 1/6th, the ratio of 1450 to 10000)
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
        for (Iterable<TitanEdge> result : results.values()) assertEquals(1, size(result));
        results = tx.multiQuery(Sets.newHashSet(qvs)).labels("connect").edges();
        for (Iterable<TitanEdge> result : results.values()) assertEquals(2, size(result));
        results = tx.multiQuery(qvs).labels("knows").edges();
        for (Iterable<TitanEdge> result : results.values()) assertEquals(0, size(result));
        results = tx.multiQuery(qvs).edges();
        for (Iterable<TitanEdge> result : results.values()) assertEquals(4, size(result));
        results2 = tx.multiQuery(qvs).properties();
        for (Iterable<TitanVertexProperty> result : results2.values()) assertEquals(1, size(result));
        results2 = tx.multiQuery(qvs).keys("name").properties();
        for (Iterable<TitanVertexProperty> result : results2.values()) assertEquals(1, size(result));

        //##################################################
        //End copied queries
        //##################################################

        newTx();

        v = (TitanVertex) getOnlyElement(tx.query().has("name", "v").vertices());
        assertNotNull(v);
        assertEquals(2, v.query().has("weight", 1.5).interval("time", 10, 30).limit(2).vertexIds().size());
        assertEquals(10, v.query().has("weight", 1.5).interval("time", 10, 30).vertexIds().size());

        newTx();

        v = (TitanVertex) getOnlyElement(tx.query().has("name", "v").vertices());
        assertNotNull(v);
        assertEquals(2, v.query().has("weight", 1.5).interval("time", 10, 30).limit(2).edgeCount());
        assertEquals(10, v.query().has("weight", 1.5).interval("time", 10, 30).edgeCount());


        newTx();
        //Test partially new vertex queries
        TitanVertex[] qvs2 = new TitanVertex[qvs.length + 2];
        qvs2[0] = tx.addVertex();
        for (int i = 0; i < qvs.length; i++) qvs2[i + 1] = getV(tx, qvs[i].longId());
        qvs2[qvs2.length - 1] = tx.addVertex();
        qvs2[0].addEdge("connect", qvs2[qvs2.length - 1]);
        qvs2[qvs2.length - 1].addEdge("connect", qvs2[0]);
        results = tx.multiQuery(qvs2).direction(IN).labels("connect").edges();
        for (Iterable<TitanEdge> result : results.values()) assertEquals(1, size(result));

    }
    //end titan-test code
}
