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
import java.util.Map;

import org.janusgraph.diskstorage.StaticBuffer;

import com.amazon.janusgraph.diskstorage.dynamodb.Constants;
import com.amazon.janusgraph.diskstorage.dynamodb.DynamoDbStoreTransaction;
import com.amazon.janusgraph.diskstorage.dynamodb.Expression;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.collect.Maps;

import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Builder for update expressions for updating multi data model entries
 * @author Michael Rodaitis
 *
 */
@Setter(AccessLevel.PUBLIC)
@Accessors(fluent = true)
@RequiredArgsConstructor
public class MultiUpdateExpressionBuilder extends AbstractBuilder {

    private static final String VALUE_LABEL = ":v";
    private static final String EXPECTED_VALUE_LABEL = ":e";
    private static final String MISSING_VALUE_EXPR = String.format("attribute_not_exists(%s)", Constants.JANUSGRAPH_VALUE);
    private static final String EXPECTED_VALUE_EXPR = String.format("%s = %s", Constants.JANUSGRAPH_VALUE, EXPECTED_VALUE_LABEL);
    private static final String SET_VALUE_EXPR = String.format("SET %s = %s", Constants.JANUSGRAPH_VALUE, VALUE_LABEL);

    private static final Map<String, String> EMPTY_ARGUMENT_NAMES = Collections.emptyMap();

    @NonNull
    private final DynamoDbStoreTransaction transaction;
    @Setter
    private StaticBuffer hashKey;
    @Setter
    private StaticBuffer rangeKey;
    @Setter
    private StaticBuffer value;

    /**
     *
     * @return
     */
    public Expression build() {
        final Map<String, AttributeValue> attributeValues = Maps.newHashMap();

        // This might be used for a DeleteItem, in which case the update expression should be null
        String updateExpression = null;
        if (value != null) {
            updateExpression = SET_VALUE_EXPR;
            final AttributeValue updateValue = encodeValue(value);
            attributeValues.put(VALUE_LABEL, updateValue);
        }

        // Condition expression and attribute value
        String conditionExpression = null;
        if (transaction.contains(hashKey, rangeKey)) {
            final StaticBuffer expectedValue = transaction.get(hashKey, rangeKey);
            if (expectedValue == null) {
                conditionExpression = MISSING_VALUE_EXPR;
            } else {
                final AttributeValue expectedAttributeValue = encodeValue(expectedValue);
                conditionExpression = EXPECTED_VALUE_EXPR;
                attributeValues.put(EXPECTED_VALUE_LABEL, expectedAttributeValue);
            }
        }

        // We aren't using any Titan column names in the expressions for MULTI records, so we don't need to label any argument names.
        return new Expression(updateExpression, conditionExpression, attributeValues);
    }

}
