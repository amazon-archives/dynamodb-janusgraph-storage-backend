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
package com.amazon.janusgraph.triple;

import lombok.Getter;

/**
 *
 * @author Addison Slabaugh
 *
 */
public class Triple {

    @Getter public static String[] line;
    @Getter public static String leftObject;
    @Getter public static String leftObjectProperty;
    @Getter public static String relationship;
    @Getter public static String rightObject;
    @Getter public static String rightObjectProperty;

    public Triple(String[] line) {
        this.line = line;
        processTriple(this.line);
    }

    /**
     * This takes a single line in a tab delimited file and splits it out by entity, relationship, and attribute
     * to form a triple.
     *
     * @param line  the line of a tab file that contains left object, right object, their properties, and relationship
     *
     */
    public void processTriple(String[] line) {
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
}
