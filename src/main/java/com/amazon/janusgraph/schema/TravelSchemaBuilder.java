package com.amazon.janusgraph.schema;

import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * Created by addisonslabaugh on 6/8/17.
 *
 * Each Titan graph has a schema comprised of the edge labels, property keys, and vertex labels used therein.
 * A Titan schema can either be explicitly or implicitly defined.
 *
 * The schema type (i.e. edge label, property key, or vertex label) is assigned to elements in
 * the graph (i.e. edge, properties or vertices respectively) when they are first created. The
 * assigned schema type cannot be changed for a particular element. This ensures a stable type
 * system that is easy to reason about.
 * ----------------------------------------------------------
 * Edge label names must be unique in the graph.
 * The multiplicity of an edge label defines a multiplicity constraint on all edges of this label,
 * that is, a maximum number of edges between pairs of vertices. For graph, you can have only
 * one biological_father, but a biological_father can have muliple children. In this case,
 * biological_father is MANY2ONE.
 *
 * The default multiplicity is MULTI.
 *
 * Multiplicity Settings and Examples:
 * MULTI: multiple edges of the same label between any pair of vertices. In other words, the graph is a multi graph.
 * SIMPLE: at most one edge of such label between any pair of vertices. In other words, the graph is a simple graph.
 * MANY2ONE: biological_father
 * ONE2MANY: winnerOf; a single person wins a competition, but an individual can win multiple competitions
 * ONE2ONE: marriedTo; a person can be married to at most one other person at a given time (with the exception
 * of polygamy?)
 * ----------------------------------------------------------
 * Property key names must be unique in the graph.  Properties on vertices and edges are key-value
 * pairs. For instance, the property name='Daniel' has the key name and the value 'Daniel'. Use cardinality
 * to define the allowed cardinality of the values associated with the key on any given vertex.
 *
 * Default cardinality setting is SINGLE
 *
 * SINGLE: at most one value per element for such key; key->value mapping is unique; birthDate
 * LIST: an arbitrary number of values per element for such key. In other words, the key is associated
 * with a list of values allowing duplicate values; sensorReading
 * SET: multiple values but no duplicate values per element for such key; name (include nicknames, maiden names,
 * etc.)
 * ----------------------------------------------------------
 * Edge labels and property keys are jointly referred to as relation types. Names of relation types must
 * be unique in the graph which means that property keys and edge labels cannot have the same name.
 *
 * Learn more about Titan schemas at http://s3.thinkaurelius.com/docs/titan/0.5.0/schema.html
 */
public class TravelSchemaBuilder {

    public static JanusGraphManagement mgmt;
    public static JanusGraph graph;
    public static String mixedIndexName;
    public static boolean uniqueNameCompositeIndex;

    public TravelSchemaBuilder(JanusGraphManagement mgmt, JanusGraph graph, String mixedIndexName, boolean uniqueNameCompositeIndex) {
        this.mgmt = mgmt;
        this.graph = graph;
        this.mixedIndexName = mixedIndexName;
        this.uniqueNameCompositeIndex = uniqueNameCompositeIndex;
        build();
    }

    public void build() {

        final PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
        JanusGraphManagement.IndexBuilder nameIndexBuilder = mgmt.buildIndex("name", Vertex.class).addKey(name);
        if (uniqueNameCompositeIndex)
            nameIndexBuilder.unique();
        JanusGraphIndex namei = nameIndexBuilder.buildCompositeIndex();
        mgmt.setConsistency(namei, ConsistencyModifier.LOCK);
        final PropertyKey age = mgmt.makePropertyKey("age").dataType(Integer.class).make();
        if (null != mixedIndexName)
            mgmt.buildIndex("vertices", Vertex.class).addKey(age).buildMixedIndex(mixedIndexName);

        final PropertyKey time = mgmt.makePropertyKey("time").dataType(Integer.class).make();
        final PropertyKey reason = mgmt.makePropertyKey("reason").dataType(String.class).make();
        final PropertyKey place = mgmt.makePropertyKey("place").dataType(Geoshape.class).make();
        if (null != mixedIndexName)
            mgmt.buildIndex("edges", Edge.class).addKey(reason).addKey(place).buildMixedIndex(mixedIndexName);

        mgmt.makeEdgeLabel("father").multiplicity(Multiplicity.MANY2ONE).make();
        mgmt.makeEdgeLabel("mother").multiplicity(Multiplicity.MANY2ONE).make();
        EdgeLabel battled = mgmt.makeEdgeLabel("battled").signature(time).make();
        mgmt.buildEdgeIndex(battled, "battlesByTime", Direction.BOTH, Order.decr, time);
        mgmt.makeEdgeLabel("lives").signature(reason).make();
        mgmt.makeEdgeLabel("pet").make();
        mgmt.makeEdgeLabel("brother").make();

        mgmt.makeVertexLabel("titan").make();
        mgmt.makeVertexLabel("location").make();
        mgmt.makeVertexLabel("god").make();
        mgmt.makeVertexLabel("demigod").make();
        mgmt.makeVertexLabel("human").make();
        mgmt.makeVertexLabel("monster").make();

        mgmt.commit();
    }
}
