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
package com.amazon.titan.diskstorage.dynamodb;

import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

/**
 * Structure of update and condition expressions required for a DynamoDB conditional update.
 *
 * The expressions share an attribute value and name map.
 * @author Alexander Patrikalakis
 * @author Michael Rodaitis
 */
public class Expression {

    private final String updateExpression;
    private final String conditionExpression;
    private final Map<String, AttributeValue> attributeValues;
    private final Map<String, String> attributeNames;

    public Expression(String updateExpression, String conditionExpression,
                            Map<String, AttributeValue> attributeValues, Map<String, String> attributeNames) {
        this.updateExpression = updateExpression;
        this.conditionExpression = conditionExpression;
        this.attributeValues = attributeValues;
        this.attributeNames = attributeNames;
    }

    public String getUpdateExpression() {
        return updateExpression;
    }

    public String getConditionExpression() {
        return conditionExpression;
    }

    public Map<String, AttributeValue> getAttributeValues() {
        // DynamoDB expects null expression maps when they are empty.
        if (attributeValues == null || attributeValues.isEmpty()) {
            return null;
        }
        return attributeValues;
    }

    public Map<String, String> getAttributeNames() {
        // DynamoDB expects null expression maps when they are empty.
        if (attributeNames == null || attributeNames.isEmpty()) {
            return null;
        }
        return attributeNames;
    }
}
