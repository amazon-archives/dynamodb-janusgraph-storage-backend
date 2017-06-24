/*
 * Copyright 2014-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.amazon.janusgraph.batch.BatchCommand;
import com.amazon.janusgraph.creator.ObjectCreationCommand;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.util.stats.MetricManager;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;

import com.amazon.janusgraph.triple.Triple;

/**
 *
 * @author Addison Slabaugh
 *
 */
public class TravelGraph implements AbstractGraph {

    public static final MetricRegistry REGISTRY = MetricManager.INSTANCE.getRegistry();
    public static final ConsoleReporter REPORTER = ConsoleReporter.forRegistry(REGISTRY).build();
    private static final int POOL_SIZE = 10;
    private static Set<Triple> triples;

    /**
     *
     * @param graph
     * @param report        allows client to determine whether or not to generate a report after ingestion
     * @throws Exception
     */
    public void load(final JanusGraph graph, boolean report) throws Exception {

        URL resource = classLoader.getResource("META-INF/HotelTriples.txt");

        triples = processFile(resource);
        for (Triple t : triples) {
            creationQueue.add(new ObjectCreationCommand(graph, t, REGISTRY, LOG));
        }

        for (int i = 0; i < POOL_SIZE; i++) {
            executor.execute(new BatchCommand(graph, creationQueue, LOG, BATCH_SIZE));
        }
        executor.shutdown();

        while (!executor.awaitTermination(60, TimeUnit.MILLISECONDS)) {
            LOG.info("Awaiting:" + creationQueue.size());
            if(report) {
                REPORTER.report();
            }
        }
        executor.shutdown();

        LOG.info("TravelGraph.load complete");
    }

    /**
     * A method that processes a file containing left and right objects, properties, and relationships and
     * returns a set of triples.
     *
     * @param url   URL that points to a resource containing left and right objects, properties
     *              and relationships
     * @return      a set of triples that can be ingested into a graph
     */
    @Override
    public Set<Triple> processFile(URL url) {
        Set<Triple> triples = new HashSet<>();
        try {
            BufferedReader bf = new BufferedReader(new InputStreamReader(url.openStream()));
            String line = "";
            while ((line = bf.readLine()) != null) {
                String[] split = line.split("\t");
                Triple t = new Triple(split);
                System.out.println(String.valueOf(t));
                triples.add(t);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return triples;
    }
}
