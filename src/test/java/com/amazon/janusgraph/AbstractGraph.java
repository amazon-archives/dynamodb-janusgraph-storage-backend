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

import com.amazon.janusgraph.triple.Triple;
import org.janusgraph.core.JanusGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Set;
import java.util.concurrent.*;


/**
 *
 * @author Addison Slabaugh
 *
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
