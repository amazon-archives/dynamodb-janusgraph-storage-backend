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

import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;

import com.amazon.janusgraph.diskstorage.dynamodb.DynamoDBStoreTransaction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;


/**
 * Builder for ExpectedAttributeValue maps for updates to SINGLE records.
 * We avoid the use of expressions here
 * because we can exceed the max expression size for very large updates.
 * @author Michael Rodaitis
 */
public class SingleExpectedAttributeValueBuilder extends AbstractBuilder {

    private DynamoDBStoreTransaction txh = null;
    private StaticBuffer key = null;

    public SingleExpectedAttributeValueBuilder transaction(DynamoDBStoreTransaction txh) {
        this.txh = txh;
        return this;
    }

    public SingleExpectedAttributeValueBuilder key(StaticBuffer key) {
        this.key = key;
        return this;
    }

    public Map<String, ExpectedAttributeValue> build(KCVMutation mutation) {
        Preconditions.checkState(txh != null, "Transaction must not be null");
        Preconditions.checkState(key != null, "Key must not be null");

        final Map<String, ExpectedAttributeValue> expected = Maps.newHashMapWithExpectedSize(mutation.getTotalMutations());

        for (Entry addedColumn : mutation.getAdditions()) {
            final StaticBuffer columnKey = addedColumn.getColumn();
            addExpectedValueIfPresent(key, columnKey, expected);
        }

        for (StaticBuffer deletedKey : mutation.getDeletions()) {
            addExpectedValueIfPresent(key, deletedKey, expected);
        }

        return expected;
    }

    private void addExpectedValueIfPresent(StaticBuffer key, StaticBuffer column, Map<String, ExpectedAttributeValue> expectedValueMap) {
        final String dynamoDbColumn = encodeKeyBuffer(column);

        if (expectedValueMap.containsKey(dynamoDbColumn)) {
            return;
        }

        if (txh.contains(key, column)) {
            final StaticBuffer expectedValue = txh.get(key, column);
            ExpectedAttributeValue expectedAttributeValue;
            if (expectedValue == null) {
                expectedAttributeValue = new ExpectedAttributeValue().withExists(false);
            } else {
                final AttributeValue attributeValue = encodeValue(expectedValue);
                expectedAttributeValue = new ExpectedAttributeValue().withValue(attributeValue)
                                                                     .withComparisonOperator(ComparisonOperator.EQ);
            }
            expectedValueMap.put(dynamoDbColumn, expectedAttributeValue);
        }
    }

}
