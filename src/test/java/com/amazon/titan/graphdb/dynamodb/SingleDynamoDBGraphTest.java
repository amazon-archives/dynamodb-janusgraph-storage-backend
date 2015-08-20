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
import static com.tinkerpop.blueprints.Direction.BOTH;
import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import org.junit.Test;

import com.amazon.titan.diskstorage.dynamodb.BackendDataModel;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.VertexList;
import com.thinkaurelius.titan.core.attribute.Contain;
import com.thinkaurelius.titan.core.attribute.Precision;
import com.thinkaurelius.titan.graphdb.types.StandardEdgeLabelMaker;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;

/**
 *
 * @author Alexander Patrikalakis
 *
 */
public class SingleDynamoDBGraphTest extends AbstractDynamoDBGraphTest {

    //begin titan-test code - modified to accommodate item size limit of SINGLE data model
    //https://github.com/thinkaurelius/titan/blob/0.5.4/titan-test/src/main/java/com/thinkaurelius/titan/graphdb/TitanGraphTest.java#L2481
    @Test @Override
    public void testVertexCentricQuery() {
        makeVertexIndexedUniqueKey("name",String.class);
        PropertyKey time = makeKey("time",Integer.class);
        PropertyKey weight = makeKey("weight",Precision.class);

        EdgeLabel author = mgmt.makeEdgeLabel("author").multiplicity(Multiplicity.MANY2ONE).unidirected().make();

        ((StandardEdgeLabelMaker)mgmt.makeEdgeLabel("connect")).sortKey(time).make();
        ((StandardEdgeLabelMaker)mgmt.makeEdgeLabel("connectDesc")).sortKey(time).sortOrder(Order.DESC).make();
        ((StandardEdgeLabelMaker)mgmt.makeEdgeLabel("friend")).sortKey(weight, time).sortOrder(Order.ASC).signature(author).make();
        ((StandardEdgeLabelMaker)mgmt.makeEdgeLabel("friendDesc")).sortKey(weight, time).sortOrder(Order.DESC).signature(author).make();
        ((StandardEdgeLabelMaker)mgmt.makeEdgeLabel("knows")).sortKey(author, weight).make();
        mgmt.makeEdgeLabel("follows").make();
        finishSchema();

        TitanVertex v = tx.addVertex();
        v.addProperty("name","v");
        TitanVertex u = tx.addVertex();
        u.addProperty("name","u");
        //changed the following line from 10000 to 1450 to accommodate DynamoDB max item size of 400kb in SINGLE model
        //TODO(Alexander Patrikalakis) refactor TitanGraphTest to have a protected method that allows backends to vary the number of vertices.
        int noVertices = 1450; //hits limit at 1480
        final double ratio = noVertices / 10000.0;
        final int round = (int) Math.round(1667 * ratio);
        assertEquals(0,noVertices%10);
        assertEquals(0,(noVertices-1)%3);
        TitanVertex[] vs = new TitanVertex[noVertices];
        for (int i = 1; i < noVertices; i++) {
            vs[i] = tx.addVertex();
            vs[i].addProperty("name", "v" + i);
        }
        EdgeLabel[] labelsV = {tx.getEdgeLabel("connect"),tx.getEdgeLabel("friend"),tx.getEdgeLabel("knows")};
        EdgeLabel[] labelsU = {tx.getEdgeLabel("connectDesc"),tx.getEdgeLabel("friendDesc"),tx.getEdgeLabel("knows")};
        for (int i = 1; i < noVertices; i++) {
            for (TitanVertex vertex : new TitanVertex[]{v,u}) {
                for (Direction d : new Direction[]{OUT,IN}) {
                    EdgeLabel label = vertex==v?labelsV[i%3]:labelsU[i%3];
                    TitanEdge e = d==OUT?vertex.addEdge(label,vs[i]):
                            vs[i].addEdge(label,vertex);
                    e.setProperty("time", i);
                    e.setProperty("weight", i % 4 + 0.5);
                    e.setProperty("name", "e" + i);
                    e.setProperty("author", i%5==0?v:vs[i % 5]);
                }
            }
        }
        int edgesPerLabel = noVertices/3;



        VertexList vl;
        Map<TitanVertex, Iterable<TitanEdge>> results;
        Map<TitanVertex, Iterable<TitanProperty>> results2;
        TitanVertex[] qvs;
        int lastTime;
        Iterator<Edge> outer;

        clopen();

        long[] vidsubset = new long[31 - 3];
        for (int i = 0; i < vidsubset.length; i++) vidsubset[i] = vs[i + 3].getLongId();
        Arrays.sort(vidsubset);

        //##################################################
        //Queries from Cache
        //##################################################
        clopen();
        for (int i = 1; i < noVertices; i++) vs[i] = tx.getVertex(vs[i].getLongId());
        v = tx.getVertex(v.getLongId());
        u = tx.getVertex(u.getLongId());
        qvs = new TitanVertex[]{vs[6], vs[9], vs[12], vs[15], vs[60]};

        //To trigger queries from cache (don't copy!!!)
        assertEquals(2*(noVertices-1), Iterables.size(v.getEdges()));


        assertEquals(10, Iterables.size(v.query().labels("connect").limit(10).vertices()));
        assertEquals(10, Iterables.size(u.query().labels("connectDesc").limit(10).vertices()));
        assertEquals(10, Iterables.size(v.query().labels("connect").has("time", Compare.GREATER_THAN, 30).limit(10).vertices()));
        assertEquals(10, Iterables.size(u.query().labels("connectDesc").has("time", Compare.GREATER_THAN, 30).limit(10).vertices()));

        lastTime = 0;
        for (Object o : v.query().labels("connect").direction(OUT).limit(20).edges()) {
            Edge e = (Edge) o;
            int nowTime = e.getProperty("time");
            assertTrue(lastTime + " vs. " + nowTime, lastTime <= nowTime);
            lastTime = nowTime;
        }
        lastTime = Integer.MAX_VALUE;
        for (Object o : u.query().labels("connectDesc").direction(OUT).limit(20).edges()) {
            Edge e = (Edge) o;
            int nowTime = e.getProperty("time");
            assertTrue(lastTime + " vs. " + nowTime, lastTime >= nowTime);
            lastTime = nowTime;
        }
        assertEquals(10, Iterables.size(v.query().labels("connect").direction(OUT).has("time", Compare.GREATER_THAN, 60).limit(10).vertices()));
        assertEquals(10, Iterables.size(u.query().labels("connectDesc").direction(OUT).has("time", Compare.GREATER_THAN, 60).limit(10).vertices()));

        outer = v.query().labels("connect").direction(OUT).limit(20).edges().iterator();
        for (Object o : v.query().labels("connect").direction(OUT).limit(10).edges()) {
            Edge e = (Edge) o;
            assertEquals(e, outer.next());
        }

        evaluateQuery(v.query().labels("connect").direction(OUT).interval("time", 3, 31),EDGE,10,1,new boolean[]{true,true});
        evaluateQuery(v.query().labels("connect").direction(OUT).has("time", 15).has("weight", 3.5),EDGE,1,1,new boolean[]{false,true});
        evaluateQuery(u.query().labels("connectDesc").direction(OUT).interval("time", 3, 31),EDGE,10,1,new boolean[]{true,true});
        assertEquals(10, v.query().labels("connect").direction(IN).interval("time", 3, 31).count());
        assertEquals(10, u.query().labels("connectDesc").direction(IN).interval("time", 3, 31).count());
        assertEquals(0, v.query().labels("connect").direction(OUT).has("time", null).count());
        assertEquals(10, v.query().labels("connect").direction(OUT).interval("time", 3, 31).vertexIds().size());
        assertEquals(edgesPerLabel-10, v.query().labels("connect").direction(OUT).has("time", Compare.GREATER_THAN, 31).count());
        assertEquals(10, Iterables.size(v.query().labels("connect").direction(OUT).interval("time", 3, 31).vertices()));
        assertEquals(3, v.query().labels("friend").direction(OUT).limit(3).count());
        evaluateQuery(v.query().labels("friend").direction(OUT).has("weight", 0.5).limit(3), EDGE, 3, 1, new boolean[]{true, true});
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 3, 33).has("weight", 0.5),EDGE,3,1,new boolean[]{true,true});
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 3, 33).has("weight", Contain.IN, ImmutableList.of(0.5)),EDGE,3,1,new boolean[]{true,true});
        evaluateQuery(v.query().labels("friend").direction(OUT).has("weight", Contain.IN, ImmutableList.of(0.5,1.5,2.5)).interval("time", 3, 33),EDGE,7,3,new boolean[]{true,true});
        evaluateQuery(v.query().labels("friend").direction(OUT).has("weight", Contain.IN, ImmutableList.of(0.5,1.5)),EDGE, round,2,new boolean[]{true,true}); //was 1667
        assertEquals(3, u.query().labels("friendDesc").direction(OUT).interval("time", 3, 33).has("weight", 0.5).count());
        assertEquals(1, v.query().labels("friend").direction(OUT).has("weight", 0.5).interval("time", 4, 10).count());
        assertEquals(1, u.query().labels("friendDesc").direction(OUT).has("weight", 0.5).interval("time", 4, 10).count());
        assertEquals(3, v.query().labels("friend").direction(OUT).interval("time", 3, 33).has("weight", 0.5).count());
        assertEquals(4, v.query().labels("friend").direction(OUT).has("time", Compare.LESS_THAN_EQUAL, 10).count());
        assertEquals(edgesPerLabel-4, v.query().labels("friend").direction(OUT).has("time", Compare.GREATER_THAN, 10).count());
        assertEquals(20, v.query().labels("friend", "connect").direction(OUT).interval("time", 3, 33).count());

        assertEquals((int)Math.ceil(edgesPerLabel/5.0), v.query().labels("knows").direction(OUT).has("author", v).count());
        assertEquals((int)Math.ceil(edgesPerLabel/5.0), v.query().labels("knows").direction(OUT).has("author", v).interval("weight", 0.0, 4.0).count());
        assertEquals((int)Math.ceil(edgesPerLabel/(5.0*2)), v.query().labels("knows").direction(OUT).has("author", v).interval("weight", 0.0, 2.0).count());
        assertEquals((int)Math.floor(edgesPerLabel/(5.0*2)), v.query().labels("knows").direction(OUT).has("author", v).interval("weight", 2.1, 4.0).count());
        assertEquals(20, Iterables.size(v.query().labels("connect", "friend").direction(OUT).interval("time", 3, 33).vertices()));
        assertEquals(20, Iterables.size(v.query().labels("connect", "friend").direction(OUT).interval("time", 3, 33).vertexIds()));
        assertEquals(30, v.query().labels("friend", "connect", "knows").direction(OUT).interval("time", 3, 33).count());
        assertEquals(noVertices-2, v.query().labels("friend", "connect", "knows").direction(OUT).has("time", Compare.NOT_EQUAL, 10).count());

        assertEquals(0, v.query().has("age", null).labels("undefined").direction(OUT).count());
        assertEquals(1, v.query().labels("connect").direction(OUT).adjacent(vs[6]).has("time", 6).count());
        assertEquals(1, v.query().labels("knows").direction(OUT).adjacent(vs[11]).count());
        assertEquals(1, v.query().labels("knows").direction(IN).adjacent(vs[11]).count());
        assertEquals(2, v.query().labels("knows").direction(BOTH).adjacent(vs[11]).count());
        assertEquals(1, v.query().labels("knows").direction(OUT).adjacent(vs[11]).has("weight", 3.5).count());
        assertEquals(2, v.query().labels("connect").adjacent(vs[6]).has("time", 6).count());
        assertEquals(0, v.query().labels("connect").adjacent(vs[8]).has("time", 8).count());

        assertEquals(edgesPerLabel, v.query().labels("connect").direction(OUT).count());
        assertEquals(edgesPerLabel, v.query().labels("connect").direction(IN).count());
        assertEquals(2*edgesPerLabel, v.query().labels("connect").direction(BOTH).count());

        assertEquals(edgesPerLabel, v.query().labels("connect").has("undefined", null).direction(OUT).count());
        assertEquals(2*(int)Math.ceil((noVertices-1)/4.0), Iterables.size(v.query().labels("connect", "friend", "knows").has("weight", 1.5).vertexIds()));
        assertEquals(1, v.query().direction(IN).has("time", 1).count());
        assertEquals(10, v.query().direction(OUT).interval("time", 4, 14).count());
        assertEquals(9, v.query().direction(IN).interval("time", 4, 14).has("time", Compare.NOT_EQUAL, 10).count());
        assertEquals(9, v.query().direction(OUT).interval("time", 4, 14).has("time", Compare.NOT_EQUAL, 10).count());
        assertEquals(noVertices-1, Iterables.size(v.query().direction(OUT).vertices()));
        assertEquals(noVertices-1, Iterables.size(v.query().direction(IN).vertices()));
        for (Direction dir : new Direction[]{IN,OUT}) {
            vl = v.query().labels().direction(dir).interval("time", 3, 31).vertexIds();
            vl.sort();
            for (int i = 0; i < vl.size(); i++) assertEquals(vidsubset[i], vl.getID(i));
        }
        assertEquals(2*(noVertices-1), Iterables.size(v.getEdges()));


        //Property queries
        assertEquals(1, Iterables.size(v.query().properties()));
        assertEquals(1, Iterables.size(v.query().keys("name").properties()));

        //MultiQueries
        results = tx.multiQuery(qvs).direction(IN).labels("connect").titanEdges();
        for (Iterable<TitanEdge> result : results.values()) assertEquals(1, Iterables.size(result));
        results = tx.multiQuery(Sets.newHashSet(qvs)).labels("connect").titanEdges();
        for (Iterable<TitanEdge> result : results.values()) assertEquals(2, Iterables.size(result));
        results = tx.multiQuery(qvs).labels("knows").titanEdges();
        for (Iterable<TitanEdge> result : results.values()) assertEquals(0, Iterables.size(result));
        results = tx.multiQuery(qvs).titanEdges();
        for (Iterable<TitanEdge> result : results.values()) assertEquals(4, Iterables.size(result));
        results2 = tx.multiQuery(qvs).properties();
        for (Iterable<TitanProperty> result : results2.values()) assertEquals(1, Iterables.size(result));
        results2 = tx.multiQuery(qvs).keys("name").properties();
        for (Iterable<TitanProperty> result : results2.values()) assertEquals(1, Iterables.size(result));

        //##################################################
        //Same queries as above but without memory loading (i.e. omitting the first query)
        //##################################################
        clopen();
        for (int i = 1; i < noVertices; i++) vs[i] = tx.getVertex(vs[i].getLongId());
        v = tx.getVertex(v.getLongId());
        u = tx.getVertex(u.getLongId());
        qvs = new TitanVertex[]{vs[6], vs[9], vs[12], vs[15], vs[60]};

        assertEquals(10, Iterables.size(v.query().labels("connect").limit(10).vertices()));
        assertEquals(10, Iterables.size(u.query().labels("connectDesc").limit(10).vertices()));
        assertEquals(10, Iterables.size(v.query().labels("connect").has("time", Compare.GREATER_THAN, 30).limit(10).vertices()));
        assertEquals(10, Iterables.size(u.query().labels("connectDesc").has("time", Compare.GREATER_THAN, 30).limit(10).vertices()));

        lastTime = 0;
        for (Object o : v.query().labels("connect").direction(OUT).limit(20).edges()) {
            Edge e = (Edge) o;
            int nowTime = e.getProperty("time");
            assertTrue(lastTime + " vs. " + nowTime, lastTime <= nowTime);
            lastTime = nowTime;
        }
        lastTime = Integer.MAX_VALUE;
        for (Object o : u.query().labels("connectDesc").direction(OUT).limit(20).edges()) {
            Edge e = (Edge) o;
            int nowTime = e.getProperty("time");
            assertTrue(lastTime + " vs. " + nowTime, lastTime >= nowTime);
            lastTime = nowTime;
        }
        assertEquals(10, Iterables.size(v.query().labels("connect").direction(OUT).has("time", Compare.GREATER_THAN, 60).limit(10).vertices()));
        assertEquals(10, Iterables.size(u.query().labels("connectDesc").direction(OUT).has("time", Compare.GREATER_THAN, 60).limit(10).vertices()));

        outer = v.query().labels("connect").direction(OUT).limit(20).edges().iterator();
        for (Object o : v.query().labels("connect").direction(OUT).limit(10).edges()) {
            Edge e = (Edge) o;
            assertEquals(e, outer.next());
        }

        evaluateQuery(v.query().labels("connect").direction(OUT).interval("time", 3, 31),EDGE,10,1,new boolean[]{true,true});
        evaluateQuery(v.query().labels("connect").direction(OUT).has("time", 15).has("weight", 3.5),EDGE,1,1,new boolean[]{false,true});
        evaluateQuery(u.query().labels("connectDesc").direction(OUT).interval("time", 3, 31),EDGE,10,1,new boolean[]{true,true});
        assertEquals(10, v.query().labels("connect").direction(IN).interval("time", 3, 31).count());
        assertEquals(10, u.query().labels("connectDesc").direction(IN).interval("time", 3, 31).count());
        assertEquals(0, v.query().labels("connect").direction(OUT).has("time", null).count());
        assertEquals(10, v.query().labels("connect").direction(OUT).interval("time", 3, 31).vertexIds().size());
        assertEquals(edgesPerLabel-10, v.query().labels("connect").direction(OUT).has("time", Compare.GREATER_THAN, 31).count());
        assertEquals(10, Iterables.size(v.query().labels("connect").direction(OUT).interval("time", 3, 31).vertices()));
        assertEquals(3, v.query().labels("friend").direction(OUT).limit(3).count());
        evaluateQuery(v.query().labels("friend").direction(OUT).has("weight", 0.5).limit(3), EDGE, 3, 1, new boolean[]{true, true});
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 3, 33).has("weight", 0.5),EDGE,3,1,new boolean[]{true,true});
        evaluateQuery(v.query().labels("friend").direction(OUT).interval("time", 3, 33).has("weight", Contain.IN, ImmutableList.of(0.5)),EDGE,3,1,new boolean[]{true,true});
        evaluateQuery(v.query().labels("friend").direction(OUT).has("weight", Contain.IN, ImmutableList.of(0.5,1.5,2.5)).interval("time", 3, 33),EDGE,7,3,new boolean[]{true,true});
        evaluateQuery(v.query().labels("friend").direction(OUT).has("weight", Contain.IN, ImmutableList.of(0.5,1.5)),EDGE,round,2,new boolean[]{true,true});
        assertEquals(3, u.query().labels("friendDesc").direction(OUT).interval("time", 3, 33).has("weight", 0.5).count());
        assertEquals(1, v.query().labels("friend").direction(OUT).has("weight", 0.5).interval("time", 4, 10).count());
        assertEquals(1, u.query().labels("friendDesc").direction(OUT).has("weight", 0.5).interval("time", 4, 10).count());
        assertEquals(3, v.query().labels("friend").direction(OUT).interval("time", 3, 33).has("weight", 0.5).count());
        assertEquals(4, v.query().labels("friend").direction(OUT).has("time", Compare.LESS_THAN_EQUAL, 10).count());
        assertEquals(edgesPerLabel-4, v.query().labels("friend").direction(OUT).has("time", Compare.GREATER_THAN, 10).count());
        assertEquals(20, v.query().labels("friend", "connect").direction(OUT).interval("time", 3, 33).count());

        assertEquals((int)Math.ceil(edgesPerLabel/5.0), v.query().labels("knows").direction(OUT).has("author", v).count());
        assertEquals((int)Math.ceil(edgesPerLabel/5.0), v.query().labels("knows").direction(OUT).has("author", v).interval("weight", 0.0, 4.0).count());
        assertEquals((int)Math.ceil(edgesPerLabel/(5.0*2)), v.query().labels("knows").direction(OUT).has("author", v).interval("weight", 0.0, 2.0).count());
        assertEquals((int)Math.floor(edgesPerLabel/(5.0*2)), v.query().labels("knows").direction(OUT).has("author", v).interval("weight", 2.1, 4.0).count());
        assertEquals(20, Iterables.size(v.query().labels("connect", "friend").direction(OUT).interval("time", 3, 33).vertices()));
        assertEquals(20, Iterables.size(v.query().labels("connect", "friend").direction(OUT).interval("time", 3, 33).vertexIds()));
        assertEquals(30, v.query().labels("friend", "connect", "knows").direction(OUT).interval("time", 3, 33).count());
        assertEquals(noVertices-2, v.query().labels("friend", "connect", "knows").direction(OUT).has("time", Compare.NOT_EQUAL, 10).count());

        assertEquals(0, v.query().has("age", null).labels("undefined").direction(OUT).count());
        assertEquals(1, v.query().labels("connect").direction(OUT).adjacent(vs[6]).has("time", 6).count());
        assertEquals(1, v.query().labels("knows").direction(OUT).adjacent(vs[11]).count());
        assertEquals(1, v.query().labels("knows").direction(IN).adjacent(vs[11]).count());
        assertEquals(2, v.query().labels("knows").direction(BOTH).adjacent(vs[11]).count());
        assertEquals(1, v.query().labels("knows").direction(OUT).adjacent(vs[11]).has("weight", 3.5).count());
        assertEquals(2, v.query().labels("connect").adjacent(vs[6]).has("time", 6).count());
        assertEquals(0, v.query().labels("connect").adjacent(vs[8]).has("time", 8).count());

        assertEquals(edgesPerLabel, v.query().labels("connect").direction(OUT).count());
        assertEquals(edgesPerLabel, v.query().labels("connect").direction(IN).count());
        assertEquals(2*edgesPerLabel, v.query().labels("connect").direction(BOTH).count());

        assertEquals(edgesPerLabel, v.query().labels("connect").has("undefined", null).direction(OUT).count());
        assertEquals(2*(int)Math.ceil((noVertices-1)/4.0), Iterables.size(v.query().labels("connect", "friend", "knows").has("weight", 1.5).vertexIds()));
        assertEquals(1, v.query().direction(IN).has("time", 1).count());
        assertEquals(10, v.query().direction(OUT).interval("time", 4, 14).count());
        assertEquals(9, v.query().direction(IN).interval("time", 4, 14).has("time", Compare.NOT_EQUAL, 10).count());
        assertEquals(9, v.query().direction(OUT).interval("time", 4, 14).has("time", Compare.NOT_EQUAL, 10).count());
        assertEquals(noVertices-1, Iterables.size(v.query().direction(OUT).vertices()));
        assertEquals(noVertices-1, Iterables.size(v.query().direction(IN).vertices()));
        for (Direction dir : new Direction[]{IN,OUT}) {
            vl = v.query().labels().direction(dir).interval("time", 3, 31).vertexIds();
            vl.sort();
            for (int i = 0; i < vl.size(); i++) assertEquals(vidsubset[i], vl.getID(i));
        }
        assertEquals(2*(noVertices-1), Iterables.size(v.getEdges()));


        //Property queries
        assertEquals(1, Iterables.size(v.query().properties()));
        assertEquals(1, Iterables.size(v.query().keys("name").properties()));

        //MultiQueries
        results = tx.multiQuery(qvs).direction(IN).labels("connect").titanEdges();
        for (Iterable<TitanEdge> result : results.values()) assertEquals(1, Iterables.size(result));
        results = tx.multiQuery(Sets.newHashSet(qvs)).labels("connect").titanEdges();
        for (Iterable<TitanEdge> result : results.values()) assertEquals(2, Iterables.size(result));
        results = tx.multiQuery(qvs).labels("knows").titanEdges();
        for (Iterable<TitanEdge> result : results.values()) assertEquals(0, Iterables.size(result));
        results = tx.multiQuery(qvs).titanEdges();
        for (Iterable<TitanEdge> result : results.values()) assertEquals(4, Iterables.size(result));
        results2 = tx.multiQuery(qvs).properties();
        for (Iterable<TitanProperty> result : results2.values()) assertEquals(1, Iterables.size(result));
        results2 = tx.multiQuery(qvs).keys("name").properties();
        for (Iterable<TitanProperty> result : results2.values()) assertEquals(1, Iterables.size(result));

        //##################################################
        //End copied queries
        //##################################################

        newTx();

        v = (TitanVertex) tx.getVertices("name", "v").iterator().next();
        assertNotNull(v);
        assertEquals(2, v.query().has("weight", 1.5).interval("time", 10, 30).limit(2).vertexIds().size());
        assertEquals(10, v.query().has("weight", 1.5).interval("time", 10, 30).vertexIds().size());

        newTx();

        v = (TitanVertex) tx.getVertices("name", "v").iterator().next();
        assertNotNull(v);
        assertEquals(2, v.query().has("weight", 1.5).interval("time", 10, 30).limit(2).count());
        assertEquals(10, v.query().has("weight", 1.5).interval("time", 10, 30).count());


        newTx();
        //Test partially new vertex queries
        TitanVertex[] qvs2 = new TitanVertex[qvs.length+2];
        qvs2[0]=tx.addVertex();
        for (int i=0;i<qvs.length;i++) qvs2[i+1]=tx.getVertex(qvs[i].getLongId());
        qvs2[qvs2.length-1]=tx.addVertex();
        qvs2[0].addEdge("connect",qvs2[qvs2.length-1]);
        qvs2[qvs2.length-1].addEdge("connect", qvs2[0]);
        results = tx.multiQuery(qvs2).direction(IN).labels("connect").titanEdges();
        for (Iterable<TitanEdge> result : results.values()) assertEquals(1, Iterables.size(result));
    }
    //end titan-test code

    @Override
    public BackendDataModel getDataModel() {
        return BackendDataModel.SINGLE;
    }
}
