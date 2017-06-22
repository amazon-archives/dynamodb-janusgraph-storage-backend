package com.amazon.janusgraph.graph;

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

public class TravelGraph implements AbstractGraph {

    public static final MetricRegistry REGISTRY = MetricManager.INSTANCE.getRegistry();
    public static final ConsoleReporter REPORTER = ConsoleReporter.forRegistry(REGISTRY).build();
    private static final int POOL_SIZE = 10;
    private static Set<Triple> triples;

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
