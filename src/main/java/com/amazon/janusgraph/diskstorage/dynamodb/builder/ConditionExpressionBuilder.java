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
import java.util.Map;

import org.janusgraph.diskstorage.StaticBuffer;

import com.amazon.janusgraph.diskstorage.dynamodb.Constants;
import com.amazon.janusgraph.diskstorage.dynamodb.Expression;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;

/**
 * ConditionExpressionBuilder is responsible for generating a KeyConditionExpressions used in the
 * DynamoDB scan and range queries.
 *
 * @author Alexander Patrikalakis
 * @author Matthew Sowders
 *
 */
public class ConditionExpressionBuilder extends AbstractBuilder {
    public static final String K = ":k";
    private static final String HASH_KEY_EQUALS = String.format("%s = %s", Constants.JANUSGRAPH_HASH_KEY, K);
    private final Map<String, String> conditionExpressions = new HashMap<>();
    private final Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();

    public ConditionExpressionBuilder hashKey(final StaticBuffer key) {
        // build up condition expression
        conditionExpressions.put(Constants.JANUSGRAPH_HASH_KEY, HASH_KEY_EQUALS);

        // add the constants
        final AttributeValue av = new AttributeValue().withS(encodeKeyBuffer(key));
        if (!expressionAttributeValues.containsKey(K)) {
            expressionAttributeValues.put(K, av);
        } else {
            if (!expressionAttributeValues.get(K).equals(av)) {
                throw new IllegalArgumentException("inconsistent hash keys provided.");
            }
        }

        return this;
    }

    public ConditionExpressionBuilder rangeKey(final StaticBuffer start, final StaticBuffer end) {
        return between(Constants.JANUSGRAPH_RANGE_KEY, start, end);
    }

    public Expression build() {
        if (conditionExpressions.isEmpty()) {
            throw new IllegalStateException("must have added at least one key condition to build");
        }
        if (!conditionExpressions.containsKey(Constants.JANUSGRAPH_HASH_KEY)) {
            throw new IllegalStateException("must have hash key in keyconditions expression");
        }
        final StringBuilder sb = new StringBuilder();
        sb.append(conditionExpressions.get(Constants.JANUSGRAPH_HASH_KEY));
        if (conditionExpressions.containsKey(Constants.JANUSGRAPH_RANGE_KEY)) {
            sb.append(" AND (").append(conditionExpressions.get(Constants.JANUSGRAPH_RANGE_KEY)).append(")");
        }
        return new Expression(null /*updateExpression*/, sb.toString(),
            expressionAttributeValues, null /*expressionAttributeNames*/);
    }

    private ConditionExpressionBuilder between(final String key, final StaticBuffer start, final StaticBuffer end) {
        final Expression filterExpression = new FilterExpressionBuilder().label(key)
                                                                         .range(start, end)
                                                                         .build();
        // build up condition expression
        conditionExpressions.put(key, filterExpression.getConditionExpression());

        // add the constants
        expressionAttributeValues.putAll(filterExpression.getAttributeValues());

        return this;
    }

}
