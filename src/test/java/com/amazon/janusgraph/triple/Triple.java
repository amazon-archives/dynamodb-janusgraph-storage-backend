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
import com.google.common.base.Preconditions;

/**
 *
 * @author Addison Slabaugh
 *
 */
@Getter
public class Triple {
    
    private String leftObject;
    private String leftObjectProperty;
    private String relationship;
    private String rightObject;
    private String rightObjectProperty;

    public Triple(String[] line) {
        Preconditions.checkArgument(line.length == 3);
        try {
            this.leftObjectProperty = line[0].split(":")[0];
            this.leftObject = line[0].split(":")[1];
            this.relationship = line[1];
            this.rightObjectProperty = line[2].split(":")[0];
            this.rightObject = line[2].split(":")[1];
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        }
    }
}
