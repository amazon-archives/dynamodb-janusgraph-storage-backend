package com.amazon.janusgraph.batch;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.janusgraph.core.JanusGraph;
import org.slf4j.Logger;

import java.util.concurrent.BlockingQueue;

/**
 * Created by addisonslabaugh on 6/13/17.
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
