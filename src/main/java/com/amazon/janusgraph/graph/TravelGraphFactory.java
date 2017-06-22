package com.amazon.janusgraph.graph;

import org.janusgraph.graphdb.database.StandardJanusGraph;

/**
 * Created by addisonslabaugh on 6/22/17.
 */
public class TravelGraphFactory implements AbstractGraphFactory {
    @Override
    public AbstractGraph createGraphFactory() {
        TravelGraph graph = new TravelGraph();
        return graph;
    }

    public static void loadGraphFactory(StandardJanusGraph graph, boolean report) throws Exception {
        TravelGraph g = new TravelGraph();
        g.load(graph, report);
    }
}
