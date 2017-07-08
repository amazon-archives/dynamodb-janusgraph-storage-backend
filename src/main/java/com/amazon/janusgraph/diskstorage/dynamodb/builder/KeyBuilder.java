/*
 * Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazon.janusgraph.diskstorage.dynamodb.builder;

import java.util.Map;

import org.janusgraph.diskstorage.StaticBuffer;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

/**
 * KeyBuilder is responsible for extracting constant string attribute names from
 * DynamoDB item maps.
 *
 * @author Matthew Sowders
 *
 */
public class KeyBuilder extends AbstractBuilder {

    private final Map<String, AttributeValue> item;

    public KeyBuilder(final Map<String, AttributeValue> item) {
        this.item = item;
    }

    public StaticBuffer build(final String name) {
        return decodeKey(item, name);
    }
}
