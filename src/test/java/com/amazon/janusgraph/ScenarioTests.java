/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.junit.Test;

import com.amazon.janusgraph.diskstorage.dynamodb.BackendDataModel;
import com.google.common.base.Stopwatch;

/**
 *
 * @author Alexander Patrikalakis
 *
 */
public class ScenarioTests {

    public static final String LABEL = "myLabel";
    public static final String BY_DATABASE_METADATA_VERSION = "byDatabaseMetadataVersion";
    public static final String DATABASE_METADATA_LABEL = "databaseMetadata";
    public static final String VERSION_PROPERTY = "version";

    /**
     * This test is to demonstrate performance in response to a report of elevated latency for committing 30 vertices.
     * http://stackoverflow.com/questions/42899388/titan-dynamodb-local-incredibly-slow-8s-commit-for-30-vertices
     * @throws BackendException
     */
    @Test
    public void performanceTest() throws BackendException {
        final Graph graph = JanusGraphFactory.open(TestGraphUtil.instance.createTestGraphConfig(BackendDataModel.MULTI));
        IntStream.of(30).forEach(i -> graph.addVertex(LABEL));
        Stopwatch watch = Stopwatch.createStarted();
        graph.tx().commit();
        System.out.println("Committing took " + watch.stop().elapsed(TimeUnit.MILLISECONDS) + " ms");
        TestGraphUtil.instance.cleanUpTables();
    }

    /**
     * http://stackoverflow.com/questions/42090616/titan-dynamodb-doesnt-release-all-acquired-locks-on-commit-via-gremlin/43742619#43742619
     * @throws BackendException
     * @throws InterruptedException
     */
    @Test
    public void lockingTest() throws BackendException, InterruptedException, ExecutionException {
        try {
            //BEGIN code from second code listing (simplified index setup)
            final Configuration config = TestGraphUtil.instance.createTestGraphConfig(BackendDataModel.MULTI);
            //default lock expiry time is 300*1000 ms = 5 minutes. Set to 100ms.
            config.setProperty("storage.lock.expiry-time", 100);
            final JanusGraph graph = JanusGraphFactory.open(config);

            //Management transaction one
            JanusGraphManagement mgmt = graph.openManagement();
            final PropertyKey propertyKey;
            if (mgmt.containsPropertyKey(VERSION_PROPERTY)) {
                propertyKey = mgmt.getPropertyKey(VERSION_PROPERTY);
            } else {
                propertyKey = mgmt.makePropertyKey(VERSION_PROPERTY).dataType(String.class).cardinality(Cardinality.SINGLE).make();
            }
            final VertexLabel labelObj;
            if (mgmt.containsVertexLabel(DATABASE_METADATA_LABEL)) {
                labelObj = mgmt.getVertexLabel(DATABASE_METADATA_LABEL);
            } else {
                labelObj = mgmt.makeVertexLabel(DATABASE_METADATA_LABEL).make();
            }
            final JanusGraphIndex index = mgmt.buildIndex(BY_DATABASE_METADATA_VERSION, Vertex.class).addKey(propertyKey).unique().indexOnly(labelObj).buildCompositeIndex();
            mgmt.setConsistency(propertyKey, ConsistencyModifier.LOCK);
            mgmt.setConsistency(index, ConsistencyModifier.LOCK);
            mgmt.commit();

            //Management transaction two
            mgmt = graph.openManagement();
            final JanusGraphIndex indexAfterFirstCommit = mgmt.getGraphIndex(BY_DATABASE_METADATA_VERSION);
            final PropertyKey propertyKeySecond = mgmt.getPropertyKey(VERSION_PROPERTY);
            if (indexAfterFirstCommit.getIndexStatus(propertyKeySecond) == SchemaStatus.INSTALLED) {
                ((ManagementSystem) mgmt).awaitGraphIndexStatus(graph, BY_DATABASE_METADATA_VERSION).status(SchemaStatus.REGISTERED).timeout(10, java.time.temporal.ChronoUnit.MINUTES).call();
            }
            mgmt.commit();

            //Management transaction three
            mgmt = graph.openManagement();
            final JanusGraphIndex indexAfterSecondCommit = mgmt.getGraphIndex(BY_DATABASE_METADATA_VERSION);
            final PropertyKey propertyKeyThird = mgmt.getPropertyKey(VERSION_PROPERTY);
            if (indexAfterSecondCommit.getIndexStatus(propertyKeyThird) != SchemaStatus.ENABLED) {
                mgmt.commit();
                mgmt = graph.openManagement();
                mgmt.updateIndex(mgmt.getGraphIndex(BY_DATABASE_METADATA_VERSION), SchemaAction.ENABLE_INDEX).get();
                mgmt.commit();
                mgmt = graph.openManagement();
                ((ManagementSystem) mgmt).awaitGraphIndexStatus(graph, BY_DATABASE_METADATA_VERSION).status(SchemaStatus.ENABLED).timeout(10, java.time.temporal.ChronoUnit.MINUTES)
                    .call();
            }
            mgmt.commit();
            //END code from second code listing (simplified index setup)

            //BEGIN code from first code listing
            graph.addVertex(T.label, DATABASE_METADATA_LABEL).property(VERSION_PROPERTY, "0.0.1");
            graph.tx().commit();
            GraphTraversalSource g = graph.traversal();
            g.V().hasLabel(DATABASE_METADATA_LABEL).has(VERSION_PROPERTY, "0.0.1").property(VERSION_PROPERTY, "0.0.2").next();
            g.tx().commit();
            Thread.sleep(100); //wait for the lock to expire
            g.V().hasLabel(DATABASE_METADATA_LABEL).has(VERSION_PROPERTY, "0.0.2").property(VERSION_PROPERTY, "0.0.3").next();
            g.tx().commit();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            TestGraphUtil.instance.cleanUpTables();
        }
    }
}

