package com.amazon.janusgraph.triple;

import java.util.HashMap;
import java.util.UUID;

/**
 * Created by addisonslabaugh on 6/10/17.
 */
public class Triple {

    static String[] line;
    static String leftObject;
    static String leftObjectProperty;
    static String relationship;
    static String rightObject;
    static String rightObjectProperty;

    public Triple(String[] line) {
        this.line = line;
        processTriple(this.line);
    }

    public String[] getLine() {
        return line;
    }

    public void processTriple(String[] line) {
        // This takes the triple (a single line in a CSV) and splits it out by entity, relationship, and attribute
        try {
            this.leftObject = line[0].split(":")[1];
            this.leftObjectProperty = line[0].split(":")[0];
            this.relationship = line[1];
            this.rightObject = line[2].split(":")[1];
            this.rightObjectProperty = line[2].split(":")[0];
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String getLeftObject() {
        return leftObject;
    }

    public static String getRelationship() {
        return relationship;
    }

    public static String getLeftObjectProperty() {
        return leftObjectProperty;
    }

    public static String getRightObject() {
        return rightObject;
    }

    public static String getRightObjectProperty() {
        return rightObjectProperty;
    }

}
