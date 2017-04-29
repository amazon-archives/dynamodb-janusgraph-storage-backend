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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.amazon.janusgraph.diskstorage.dynamodb.Constants;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.janusgraph.diskstorage.StaticBuffer;

/**
 * ItemBuilder is responsible for translating from StaticBuffers to DynamoDB
 * item maps.
 *
 * @author Matthew Sowders
 *
 */
public class ItemBuilder extends AbstractBuilder {

    private Map<String, AttributeValue> item = new HashMap<>();

    public ItemBuilder hashKey(StaticBuffer key) {
        item.put(Constants.TITAN_HASH_KEY, encodeKeyAsAttributeValue(key));
        return this;
    }

    public ItemBuilder rangeKey(StaticBuffer key) {
        item.put(Constants.TITAN_RANGE_KEY, encodeKeyAsAttributeValue(key));
        return this;
    }

    public ItemBuilder value(StaticBuffer value) {
        item.put(Constants.TITAN_VALUE, encodeValue(value));
        return this;
    }

    public Map<String, AttributeValue> build() {
        return Collections.unmodifiableMap(item);
    }

}
