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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;

import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;

/**
 * UpdateBuilder is responsible for building AttributeValueUpdate maps to update
 * items in DynamoDB in the SINGLE data model.
 *
 * @author Matthew Sowders
 */
public class SingleUpdateBuilder extends AbstractBuilder {
    private final Map<String, AttributeValueUpdate> updates = new HashMap<>();

    public SingleUpdateBuilder put(final StaticBuffer column, final StaticBuffer value) {
        updates.put(encodeKeyBuffer(column),
                new AttributeValueUpdate()
                        .withAction(AttributeAction.PUT)
                        .withValue(encodeValue(value)));
        return this;
    }

    public SingleUpdateBuilder additions(final List<Entry> additions) {
        for (Entry addition : additions) {
            put(addition.getColumn(), addition.getValue());
        }
        return this;
    }

    private SingleUpdateBuilder delete(final StaticBuffer column) {
        updates.put(encodeKeyBuffer(column),
                new AttributeValueUpdate()
                        .withAction(AttributeAction.DELETE));
        return this;
    }

    public Map<String, AttributeValueUpdate> build() {
        return new HashMap<>(updates);
    }

    public SingleUpdateBuilder deletions(final List<StaticBuffer> deletions) {
        for (StaticBuffer deletion : deletions) {
            delete(deletion);
        }
        return this;
    }
}
