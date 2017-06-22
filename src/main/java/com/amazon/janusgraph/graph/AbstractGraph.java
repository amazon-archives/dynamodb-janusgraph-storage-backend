package com.amazon.janusgraph.graph;

import com.amazon.janusgraph.triple.Triple;
import org.janusgraph.core.JanusGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Set;
import java.util.concurrent.*;


/**
 * Created by addisonslabaugh on 6/21/17.
 */
public interface AbstractGraph {

    int BATCH_SIZE = 10;
    Logger LOG = LoggerFactory.getLogger(AbstractGraph.class);
    int POOL_SIZE = 10;

    ClassLoader classLoader = TravelGraph.class.getClassLoader();
    BlockingQueue<Runnable> creationQueue = new LinkedBlockingQueue<>();
    ExecutorService executor = Executors.newFixedThreadPool(POOL_SIZE);

    static void load(final JanusGraph graph, boolean report) throws Exception {
    }

    public abstract Set<Triple> processFile(URL url);
}
