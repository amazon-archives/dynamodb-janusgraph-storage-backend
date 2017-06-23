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
package com.amazon.janusgraph.batch;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.janusgraph.core.JanusGraph;
import org.slf4j.Logger;

import java.util.concurrent.BlockingQueue;

/**
 *
 * @author Addison Slabaugh
 *
 */
public class BatchCommand implements Runnable {

    final JanusGraph graph;
    final BlockingQueue<Runnable> commands;
    public static Logger LOG;
    public static int BATCH_SIZE;

    public BatchCommand(JanusGraph graph, BlockingQueue<Runnable> commands, Logger LOG, int BATCH_SIZE) {
        this.graph = graph;
        this.commands = commands;
        this.LOG = LOG;
        this.BATCH_SIZE = BATCH_SIZE;
    }

    @Override
    public void run() {
        int i = 0;
        Runnable command = null;
        while ((command = commands.poll()) != null) {
            try {
                command.run();
            } catch (Throwable e) {
                Throwable rootCause = ExceptionUtils.getRootCause(e);
                String rootCauseMessage = null == rootCause ? "" : rootCause.getMessage();
                LOG.error("Error processing line {} {}", e.getMessage(), rootCauseMessage, e);
            }
            if (i++ % BATCH_SIZE == 0) {
                try {
                    graph.tx().commit();
                } catch (Throwable e) {
                    LOG.error("Error processing commit {} {}", e.getMessage(), ExceptionUtils.getRootCause(e).getMessage());
                }
            }
        }

        try {
            graph.tx().commit();
        } catch (Throwable e) {
            LOG.error("Error processing commit {} {}", e.getMessage(), ExceptionUtils.getRootCause(e).getMessage());
        }
    }

}
