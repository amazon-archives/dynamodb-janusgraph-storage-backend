package com.amazon.janusgraph.graph;

import org.janusgraph.graphdb.database.StandardJanusGraph;

/**
 * Created by addisonslabaugh on 6/22/17.
 */
public interface AbstractGraphFactory {
    public AbstractGraph createGraphFactory();
}
