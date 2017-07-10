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

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.inV;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.keycolumnvalue.cache.CacheTransaction;
import org.janusgraph.diskstorage.locking.TemporaryLockingException;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.amazon.janusgraph.diskstorage.dynamodb.BackendDataModel;
import com.amazon.janusgraph.diskstorage.dynamodb.DynamoDbStoreTransaction;
import com.amazon.janusgraph.testcategory.IsolateRemainingTestsCategory;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.opencsv.CSVReader;

import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author Alexander Patrikalakis
 * @author John Stephenson
 * @author Addison Slabaugh
 *
 */
@Slf4j
@Category({IsolateRemainingTestsCategory.class})
public class ScenarioTests {

    private static final String LABEL = "myLabel";
    private static final String BY_DATABASE_METADATA_VERSION = "byDatabaseMetadataVersion";
    private static final String DATABASE_METADATA_LABEL = "databaseMetadata";
    private static final String VERSION_PROPERTY = "version";
    private static final boolean USE_STORAGE_NATIVE_LOCKING = true;
    private static final boolean USE_JANUSGRAPH_LOCKING = false;
    private static final boolean USE_GRAPHINDEX_LOCKING = true;
    private static final boolean USE_EDGESTORE_LOCKING = true;
    private static final String VERSION_ONE = "0.0.1";
    private static final String VERSION_TWO = "0.0.2";
    private static final String VERSION_THREE = "0.0.3";

    /**
     * This test is to demonstrate performance in response to a report of elevated latency for committing 30 vertices.
     * http://stackoverflow.com/questions/42899388/titan-dynamodb-local-incredibly-slow-8s-commit-for-30-vertices
     * @throws BackendException in case cleanUpTables fails
     */
    @Test
    public void performanceTest() throws BackendException {
        final Graph graph = JanusGraphFactory.open(TestGraphUtil.instance.createTestGraphConfig(BackendDataModel.MULTI));
        IntStream.of(30).forEach(i -> graph.addVertex(LABEL));
        final Stopwatch watch = Stopwatch.createStarted();
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
    public void lockingTest() throws BackendException, InterruptedException {
        createSchemaAndDemoLockExpiry(USE_STORAGE_NATIVE_LOCKING, USE_EDGESTORE_LOCKING, USE_GRAPHINDEX_LOCKING, 100);
    }

    /**
     * https://stackoverflow.com/questions/44535054/generalizing-dynamodb-janusgraph-factory-lock-and-schema-problems
     */
    public enum Relationship {
        instanceOf, hotelBrandType
    }
    @Getter
    @ToString
    public class Triple {
        private final String leftPropertyValue;
        private final String leftPropertyName;
        private final Relationship relationship;
        private final String rightPropertyValue;
        private final String rightPropertyName;

        Triple(final String[] line) {
            Preconditions.checkArgument(line.length == 3);
            final String[] left = line[0].split(":");
            final String[] right = line[2].split(":");
            this.leftPropertyName = left[0];
            this.leftPropertyValue = left[1];
            this.relationship = Relationship.valueOf(line[1]);
            this.rightPropertyName = right[0];
            this.rightPropertyValue = right[1];
        }
    }

    private static void createHotelSchema(final JanusGraph graph) {
        //another issue, you should only try to create the schema once.
        //you use uniqueness constraints, so you need to define the schema up front with the unique() call,
        //but if you did not use uniqueness constraints, you could just let JanusGraph create the schema for you.
        final JanusGraphManagement mgmt = graph.openManagement();
        final PropertyKey brandtypePropertyKey = mgmt.makePropertyKey("brandtype").dataType(String.class).make();
        mgmt.buildIndex("brandtypeIndex", Vertex.class).addKey(brandtypePropertyKey).unique().buildCompositeIndex();
        final PropertyKey namePropertyKey = mgmt.makePropertyKey("name").dataType(String.class).make();
        mgmt.buildIndex("nameIndex", Vertex.class).addKey(namePropertyKey).unique().buildCompositeIndex();
        mgmt.makeEdgeLabel(Relationship.hotelBrandType.name()).multiplicity(Multiplicity.MANY2ONE).make();
        mgmt.makeEdgeLabel(Relationship.instanceOf.name()).multiplicity(Multiplicity.MANY2ONE).make();
        mgmt.commit();
    }

    private void tripleIngestBase(final BiConsumer<StandardJanusGraph, List<Triple>> writer) throws BackendException {
        final Stopwatch watch = Stopwatch.createStarted();
        final StandardJanusGraph graph = (StandardJanusGraph) JanusGraphFactory.open(TestGraphUtil.instance.createTestGraphConfig(BackendDataModel.MULTI));
        log.info("Created graph in " + watch.elapsed(TimeUnit.MILLISECONDS) + " ms");
        watch.reset();
        watch.start();
        createHotelSchema(graph);
        log.info("Created schema in " + watch.elapsed(TimeUnit.MILLISECONDS) + " ms");

        watch.reset();
        watch.start();
        final URL url = ScenarioTests.class.getClassLoader().getResource("META-INF/HotelTriples.txt");
        Preconditions.checkNotNull(url);
        final List<Triple> lines;
        try (CSVReader reader = new CSVReader(new InputStreamReader(url.openStream()))) {
            lines = reader.readAll().stream()
                .map(Triple::new)
                .collect(Collectors.toList());
        } catch (IOException e) {
            throw new IllegalStateException("Error processing triple file", e);
        }
        log.info("Read file into Triple objects in " + watch.elapsed(TimeUnit.MILLISECONDS) + " ms");
        watch.reset();
        watch.start();
        writer.accept(graph, lines);
        log.info("Added objects in " + watch.elapsed(TimeUnit.MILLISECONDS) + " ms");
        TestGraphUtil.instance.cleanUpTables();
    }

    @Test
    public void processTripleWithTraversals() throws BackendException {
        tripleIngestBase((StandardJanusGraph graph, List<Triple> lines) -> {
            final GraphTraversalSource g = graph.traversal();
            lines.parallelStream().forEach(triple -> {
                final Vertex left = getVertexIfDoesntExist(g, triple.getLeftPropertyName(),
                    triple.getLeftPropertyValue());
                final Vertex right = getVertexIfDoesntExist(g, triple.getRightPropertyName(),
                    triple.getRightPropertyValue());
                //your original method was creating vertices in the processRelationship method.
                //this caused the uniqueness constraint violation (one of a few issues in your
                //original code) because you have a unique index on the rightPropertyName
                g.V()
                    .is(left)
                    .outE(triple.getRelationship().name())
                    .filter(inV().is(right))
                    .tryNext()
                    .orElseGet(() -> left.addEdge(triple.getRelationship().name(), right));
            });
            final Stopwatch watch = Stopwatch.createStarted();
            g.tx().commit();
            log.info("Committed in " + watch.elapsed(TimeUnit.MILLISECONDS) + " ms");
            watch.stop();
        });
    }

    private static Vertex getVertexIfDoesntExist(final GraphTraversalSource g,
        final String propertyName, final String propertyValue) {
        return g.V()
            .has(propertyName, propertyValue)
            .tryNext()
            .orElseGet(() -> g.addV().property(propertyName, propertyValue).next());
    }

    @Test
    public void processTripleWithMaps() throws BackendException {
        final ConcurrentMap<String, Vertex> brandTypeMap = new ConcurrentHashMap<>();
        final ConcurrentMap<String, Vertex> companyMap = new ConcurrentHashMap<>();
        final ConcurrentMap<String, Vertex> hotelBrandMap = new ConcurrentHashMap<>();
        tripleIngestBase((StandardJanusGraph graph, List<Triple> triples) -> {
            final JanusGraphTransaction threadedGraph = graph.newTransaction();
            triples.parallelStream().forEach(triple -> {
                final Vertex outV = hotelBrandMap.computeIfAbsent(triple.getLeftPropertyValue(),
                    value -> threadedGraph.addVertex(triple.getLeftPropertyName(), value));
                //your original method was creating vertices in the processRelationship method.
                //this caused the uniqueness constraint violation (one of a few issues in your
                //original code) because you have a unique index on the rightPropertyName
                switch (triple.getRelationship()) {
                    case hotelBrandType:
                        outV.addEdge("hotelBrandType",brandTypeMap.computeIfAbsent(triple.getRightPropertyValue(),
                            value -> threadedGraph.addVertex(triple.getRightPropertyName(), value)));
                        break;
                    case instanceOf:
                        outV.addEdge("instanceOf", companyMap.computeIfAbsent(triple.getRightPropertyValue(),
                            value -> threadedGraph.addVertex(triple.getRightPropertyName(), value)));
                        break;
                    default:
                        throw new IllegalArgumentException("unexpected relationship type");
                }
            });
            final Stopwatch watch = Stopwatch.createStarted();
            threadedGraph.commit();
            log.info("Committed in " + watch.elapsed(TimeUnit.MILLISECONDS) + " ms");
            watch.stop();
        });
    }

    @Test
    public void demoNonnativeLockersWithDynamoDB() throws BackendException, InterruptedException {
        createSchemaAndDemoLockExpiry(USE_JANUSGRAPH_LOCKING, USE_EDGESTORE_LOCKING, USE_GRAPHINDEX_LOCKING, 200);
    }

    private void createSchemaAndDemoLockExpiry(final boolean useNativeLocking, final boolean useEdgestoreLocking, final boolean useGraphindexLocking, final long waitMillis) throws BackendException {
        try {
            final StandardJanusGraph graph = (StandardJanusGraph) createGraphWithSchema(useNativeLocking, useEdgestoreLocking, useGraphindexLocking, waitMillis);
            demonstrateLockExpiry(graph, useNativeLocking, waitMillis);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            TestGraphUtil.instance.cleanUpTables();
        }
    }

    private static DynamoDbStoreTransaction getStoreTransaction(final ManagementSystem mgmt) {
        return DynamoDbStoreTransaction.getTx(((CacheTransaction) mgmt.getWrappedTx().getTxHandle().getStoreTransaction()).getWrappedTransaction());
    }

    private JanusGraph createGraphWithSchema(final boolean useNativeLocking, final boolean useVersionPropertyLocking,
        final boolean useMetadataVersionIndexLocking, final long waitMillis)
        throws InterruptedException, ExecutionException {
        //http://stackoverflow.com/questions/42090616/titan-dynamodb-doesnt-release-all-acquired-locks-on-commit-via-gremlin/43742619#43742619
        //BEGIN code from second code listing (simplified index setup)
        final Configuration config = TestGraphUtil.instance.createTestGraphConfig(BackendDataModel.MULTI);
        //default lock expiry time is 300*1000 ms = 5 minutes. Set to 200ms.
        config.setProperty("storage.lock.expiry-time", waitMillis);
        if(!useNativeLocking) {
            //Use JanusGraph locking
            config.setProperty("storage.dynamodb.native-locking", false);

            //Edgestore lock table
            config.setProperty("storage.dynamodb.stores.edgestore_lock_.data-model", BackendDataModel.MULTI.name());
            config.setProperty("storage.dynamodb.stores.edgestore_lock_.scan-limit", 10000);
            config.setProperty("storage.dynamodb.stores.edgestore_lock_.initial-read-capacity", 10);
            config.setProperty("storage.dynamodb.stores.edgestore_lock_.read-rate", 10);
            config.setProperty("storage.dynamodb.stores.edgestore_lock_.initial-write-capacity", 10);
            config.setProperty("storage.dynamodb.stores.edgestore_lock_.write-rate", 10);

            //Graphindex lock table
            config.setProperty("storage.dynamodb.stores.graphindex_lock_.data-model", BackendDataModel.MULTI.name());
            config.setProperty("storage.dynamodb.stores.graphindex_lock_.scan-limit", 10000);
            config.setProperty("storage.dynamodb.stores.graphindex_lock_.initial-read-capacity", 10);
            config.setProperty("storage.dynamodb.stores.graphindex_lock_.read-rate", 10);
            config.setProperty("storage.dynamodb.stores.graphindex_lock_.initial-write-capacity", 10);
            config.setProperty("storage.dynamodb.stores.graphindex_lock_.write-rate", 10);

            //system_properties lock table
            config.setProperty("storage.dynamodb.stores.system_properties_lock_.data-model", BackendDataModel.MULTI.name());
            config.setProperty("storage.dynamodb.stores.system_properties_lock_.scan-limit", 10000);
            config.setProperty("storage.dynamodb.stores.system_properties_lock_.initial-read-capacity", 1);
            config.setProperty("storage.dynamodb.stores.system_properties_lock_.read-rate", 1);
            config.setProperty("storage.dynamodb.stores.system_properties_lock_.initial-write-capacity", 1);
            config.setProperty("storage.dynamodb.stores.system_properties_lock_.write-rate", 1);
        }
        final JanusGraph graph = JanusGraphFactory.open(config);

        //Management transaction one
        ManagementSystem mgmt = (ManagementSystem) graph.openManagement();
        if(useNativeLocking) {
            System.out.println("mgmt tx one " + getStoreTransaction(mgmt).toString());
        }
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
        if (useVersionPropertyLocking) {
            mgmt.setConsistency(propertyKey, ConsistencyModifier.LOCK);
        }
        if (useMetadataVersionIndexLocking) {
            mgmt.setConsistency(index, ConsistencyModifier.LOCK);
        }
        mgmt.commit();

        //Management transaction two
        mgmt = (ManagementSystem) graph.openManagement();
        if(useNativeLocking) {
            System.out.println("mgmt tx two " + getStoreTransaction(mgmt).toString());
        }
        final JanusGraphIndex indexAfterFirstCommit = mgmt.getGraphIndex(BY_DATABASE_METADATA_VERSION);
        final PropertyKey propertyKeySecond = mgmt.getPropertyKey(VERSION_PROPERTY);
        if (indexAfterFirstCommit.getIndexStatus(propertyKeySecond) == SchemaStatus.INSTALLED) {
            ManagementSystem.awaitGraphIndexStatus(graph, BY_DATABASE_METADATA_VERSION).status(SchemaStatus.REGISTERED).timeout(10, java.time.temporal.ChronoUnit.MINUTES).call();
        }
        mgmt.commit();

        //Management transaction three
        mgmt = (ManagementSystem) graph.openManagement();
        if(useNativeLocking) {
            System.out.println("mgmt tx three " + getStoreTransaction(mgmt).toString());
        }
        final JanusGraphIndex indexAfterSecondCommit = mgmt.getGraphIndex(BY_DATABASE_METADATA_VERSION);
        final PropertyKey propertyKeyThird = mgmt.getPropertyKey(VERSION_PROPERTY);
        if (indexAfterSecondCommit.getIndexStatus(propertyKeyThird) != SchemaStatus.ENABLED) {
            mgmt.commit();
            mgmt = (ManagementSystem) graph.openManagement();
            if(useNativeLocking) {
                System.out.println("mgmt tx four " + getStoreTransaction(mgmt).toString());
            }
            mgmt.updateIndex(mgmt.getGraphIndex(BY_DATABASE_METADATA_VERSION), SchemaAction.ENABLE_INDEX).get();
            mgmt.commit();
            mgmt = (ManagementSystem) graph.openManagement();
            if(useNativeLocking) {
                System.out.println("mgmt tx five " + getStoreTransaction(mgmt).toString());
            }
            ManagementSystem.awaitGraphIndexStatus(graph, BY_DATABASE_METADATA_VERSION).status(SchemaStatus.ENABLED).timeout(10, java.time.temporal.ChronoUnit.MINUTES)
                .call();
        }
        mgmt.commit();
        //END code from second code listing (simplified index setup)
        return graph;
    }

    private static DynamoDbStoreTransaction getTxFromGraph(final StandardJanusGraph graph) {
        return DynamoDbStoreTransaction.getTx(((CacheTransaction) ((StandardJanusGraphTx) graph.getCurrentThreadTx()).getTxHandle().getStoreTransaction()).getWrappedTransaction());
    }

    private void demonstrateLockExpiry(final StandardJanusGraph graph, final boolean useNativeLocking, final long waitMillis) throws TemporaryLockingException, InterruptedException {
        //BEGIN code from first code listing
        graph.addVertex(T.label, DATABASE_METADATA_LABEL).property(VERSION_PROPERTY, VERSION_ONE);
        if(useNativeLocking) {
            System.out.println("regular tx one " + getTxFromGraph(graph).toString() + " " + System.currentTimeMillis());
        }
        graph.tx().commit();

        final GraphTraversalSource g = graph.traversal();
        g.V().hasLabel(DATABASE_METADATA_LABEL).has(VERSION_PROPERTY, VERSION_ONE).property(VERSION_PROPERTY, VERSION_TWO).next();
        if(useNativeLocking) {
            System.out.println("regular tx two " + getTxFromGraph(graph).toString() + " " + System.currentTimeMillis());
        }
        g.tx().commit();

        Thread.sleep(waitMillis); //wait for the lock to expire
        g.V().hasLabel(DATABASE_METADATA_LABEL).has(VERSION_PROPERTY, VERSION_TWO).property(VERSION_PROPERTY, VERSION_THREE).next();
        if(useNativeLocking) {
            System.out.println("regular tx three " + getTxFromGraph(graph).toString() + " " + System.currentTimeMillis());
        }
        g.tx().commit();
        //END code from first code listing
    }
}

