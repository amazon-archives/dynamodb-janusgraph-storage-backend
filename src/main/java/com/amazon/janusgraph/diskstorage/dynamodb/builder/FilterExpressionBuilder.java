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
package com.amazon.janusgraph.diskstorage.dynamodb.builder;

import java.util.HashMap;
import java.util.Map;

import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;

import com.amazon.janusgraph.diskstorage.dynamodb.Constants;
import com.amazon.janusgraph.diskstorage.dynamodb.Expression;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Builds filter expressions using the encoding of Titan data model
 * @author Michael Rodaitis
 *
 */
public class FilterExpressionBuilder extends AbstractBuilder {

    private final Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
    @Setter
    @Accessors(fluent = true)
    private String label;
    private StaticBuffer startValue;
    private StaticBuffer endValue;

    public FilterExpressionBuilder rangeKey() {
        return label(Constants.JANUSGRAPH_RANGE_KEY);
    }

    public FilterExpressionBuilder range(final StaticBuffer start, final StaticBuffer end) {
        this.startValue = start;
        this.endValue = end;
        return this;
    }

    public FilterExpressionBuilder range(final SliceQuery slice) {
        this.startValue = slice.getSliceStart();
        this.endValue = slice.getSliceEnd();
        return this;
    }

    public Expression build() {

        final StringBuilder sb = new StringBuilder();
        sb.append(label).append(" BETWEEN ");

        final String startVar = String.format(":s%s", label);
        final String endVar = String.format(":e%s", label);
        sb.append(startVar).append(" AND ").append(endVar);
        expressionAttributeValues.put(startVar, encodeKeyAsAttributeValue(startValue));
        expressionAttributeValues.put(endVar, encodeKeyAsAttributeValue(endValue));

        return new Expression(null /*updateExpression*/, sb.toString(),
            expressionAttributeValues, null /*expressionAttributeNames*/);
    }

}
