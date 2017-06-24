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

import org.janusgraph.graphdb.database.StandardJanusGraph;

/**
 *
 * @author Addison Slabaugh
 *
 */
public class TravelGraphFactory implements AbstractGraphFactory {
    @Override
    public AbstractGraph createGraphFactory() {
        TravelGraph graph = new TravelGraph();
        return graph;
    }

    /**
     * This method creates a new TravelGraph instance and loads it. The client can specify whether or not
     * to generate a report after ingestion.
     *
     * @param graph
     * @param report        indicates whether or not to produce a report after ingestion
     * @throws Exception
     */
    public static void loadGraphFactory(StandardJanusGraph graph, boolean report) throws Exception {
        TravelGraph g = new TravelGraph();
        g.load(graph, report);
    }
}
