package com.amazon.janusgraph.creator;

import com.codahale.metrics.MetricRegistry;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import com.amazon.janusgraph.triple.Triple;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.slf4j.Logger;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;


/**
 * Created by addisonslabaugh on 6/13/17.
 */
public class ObjectCreationCommand implements Runnable {

    public static JanusGraph graph;
    private static Triple triple;
    private static MetricRegistry REGISTRY;
    public static Logger LOG;
    private static final String TIMER_LINE = "TravelGraph.line";
    private static final String TIMER_CREATE = "TravelGraph.create_";
    private static final String COUNTER_GET = "TravelGraph.get_";

    public ObjectCreationCommand(JanusGraph graph, Triple triple, MetricRegistry REGISTRY, Logger LOG) {
        this.graph = graph;
        this.triple = triple;
        this.REGISTRY = REGISTRY;
        this.LOG = LOG;
    }

    @Override
    public void run() {

        graph.tx().rollback();
        JanusGraphManagement mgmt = graph.openManagement();
        if (mgmt.getGraphIndex(triple.getRightObjectProperty()) == null) {
            final PropertyKey hotelKey = mgmt.makePropertyKey(triple.getRightObjectProperty()).dataType(String.class).make();
            mgmt.buildIndex(triple.getRightObjectProperty(), Vertex.class).addKey(hotelKey).unique().buildCompositeIndex();
        }

        if (mgmt.getEdgeLabel(triple.getRelationship()) == null) {
            mgmt.makeEdgeLabel(triple.getRelationship()).multiplicity(Multiplicity.MANY2ONE).make();
        }

        if (mgmt.getGraphIndex(triple.getLeftObjectProperty()) == null) {
            final PropertyKey brandTypeKey = mgmt.makePropertyKey(triple.getLeftObjectProperty()).dataType(String.class).make();
            mgmt.buildIndex(triple.getLeftObjectProperty(), Vertex.class).addKey(brandTypeKey).unique().buildCompositeIndex();
        }
        mgmt.commit();

        long start = System.currentTimeMillis();

        String RIGHT_OBJECT_PROPERTY = triple.getRightObjectProperty();
        Vertex rightObject = graph.addVertex();
        rightObject.property(RIGHT_OBJECT_PROPERTY, triple.getRightObject());
        REGISTRY.counter(COUNTER_GET + RIGHT_OBJECT_PROPERTY).inc();

        String LEFT_OBJECT_PROPERTY = triple.getLeftObjectProperty();
        Vertex leftObject = graph.addVertex();
        rightObject.property(LEFT_OBJECT_PROPERTY, triple.getLeftObject());
        REGISTRY.counter(COUNTER_GET + LEFT_OBJECT_PROPERTY).inc();

        try {
            processRelationship(graph, triple);
        } catch (Throwable e) {
            Throwable rootCause = ExceptionUtils.getRootCause(e);
            String rootCauseMessage = null == rootCause ? "" : rootCause.getMessage();
            LOG.error("Error processing line {} {}", e.getMessage(), rootCauseMessage, e);
        }

        long end = System.currentTimeMillis();
        long time = end - start;
        REGISTRY.timer(TIMER_CREATE + RIGHT_OBJECT_PROPERTY).update(time, TimeUnit.MILLISECONDS);
    }

    private static void processRelationship(JanusGraph graph, Triple triple) {
        Vertex left = get(graph, triple.getLeftObjectProperty(), triple.getLeftObject());
        if (null == left) {
            REGISTRY.counter("error.missingLeftObject." + triple.getLeftObject()).inc();
            left = graph.addVertex();
            left.property(triple.getLeftObjectProperty(), triple.getLeftObject());
        }
        Vertex right = get(graph, triple.getRightObjectProperty(), triple.getRightObject());
        if (null == right) {
            REGISTRY.counter("error.missingRightObject." + triple.getRightObject()).inc();
            right = graph.addVertex();
            right.property(triple.getRightObjectProperty(), triple.getRightObject());
        }
        left.addEdge(triple.getRelationship(), right);
    }

    private static Vertex get(final JanusGraph graph, final String key, final String value) {
        final GraphTraversalSource g = graph.traversal();
        final Iterator<Vertex> it = g.V().has(key, value);
        return it.hasNext() ? it.next() : null;
    }

}
