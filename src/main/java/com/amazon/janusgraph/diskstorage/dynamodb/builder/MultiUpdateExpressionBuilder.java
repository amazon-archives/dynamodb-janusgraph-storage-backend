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

import com.amazon.janusgraph.diskstorage.dynamodb.Constants;
import com.amazon.janusgraph.diskstorage.dynamodb.DynamoDBStoreTransaction;
import com.amazon.janusgraph.diskstorage.dynamodb.Expression;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.collect.Maps;
import org.janusgraph.diskstorage.StaticBuffer;

/**
 * Builder for update expressions for updating multi data model entries
 * @author Michael Rodaitis
 *
 */
public class MultiUpdateExpressionBuilder extends AbstractBuilder {

    private static final String VALUE_LABEL = ":v";
    private static final String EXPECTED_VALUE_LABEL = ":e";
    private static final String MISSING_VALUE_EXPR = String.format("attribute_not_exists(%s)", Constants.TITAN_VALUE);
    private static final String EXPECTED_VALUE_EXPR = String.format("%s = %s", Constants.TITAN_VALUE, EXPECTED_VALUE_LABEL);
    private static final String SET_VALUE_EXPR = String.format("SET %s = %s", Constants.TITAN_VALUE, VALUE_LABEL);

    private static final Map<String, String> EMPTY_ARGUMENT_NAMES = Collections.emptyMap();

    private DynamoDBStoreTransaction transaction;
    private StaticBuffer hashKey;
    private StaticBuffer rangeKey;
    private StaticBuffer value;

    public MultiUpdateExpressionBuilder transaction(DynamoDBStoreTransaction transaction) {
        this.transaction = transaction;
        return this;
    }

    public MultiUpdateExpressionBuilder hashKey(StaticBuffer hashKey) {
        this.hashKey = hashKey;
        return this;
    }

    public MultiUpdateExpressionBuilder rangeKey(StaticBuffer rangeKey) {
        this.rangeKey = rangeKey;
        return this;
    }

    public MultiUpdateExpressionBuilder value(StaticBuffer value) {
        this.value = value;
        return this;
    }

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
        return new Expression(updateExpression, conditionExpression, attributeValues, EMPTY_ARGUMENT_NAMES);
    }

}
