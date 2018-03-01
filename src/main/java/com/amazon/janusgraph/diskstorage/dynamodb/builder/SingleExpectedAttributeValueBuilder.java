/*
 * Copyright 2014-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import lombok.NonNull;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;

import com.amazon.janusgraph.diskstorage.dynamodb.DynamoDbSingleRowStore;
import com.amazon.janusgraph.diskstorage.dynamodb.DynamoDbStoreTransaction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.google.common.collect.Maps;

import lombok.RequiredArgsConstructor;


/**
 * Builder for ExpectedAttributeValue maps for updates to SINGLE records.
 * We avoid the use of expressions here
 * because we can exceed the max expression size for very large updates.
 * @author Michael Rodaitis
 * @author Alexander Patrikalakis
 */
@RequiredArgsConstructor
public class SingleExpectedAttributeValueBuilder extends AbstractBuilder {

    @NonNull
    private final DynamoDbSingleRowStore store;
    @NonNull
    private final DynamoDbStoreTransaction transaction;
    @NonNull
    private final StaticBuffer key;

    public Map<String, ExpectedAttributeValue> build(final KCVMutation mutation) {
        final Map<String, ExpectedAttributeValue> expected = Maps.newHashMapWithExpectedSize(mutation.getTotalMutations());

        for (Entry addedColumn : mutation.getAdditions()) {
            final StaticBuffer columnKey = addedColumn.getColumn();
            addExpectedValueIfPresent(columnKey, expected);
        }

        for (StaticBuffer deletedKey : mutation.getDeletions()) {
            addExpectedValueIfPresent(deletedKey, expected);
        }

        return expected;
    }

    private void addExpectedValueIfPresent(final StaticBuffer column, final Map<String, ExpectedAttributeValue> expectedValueMap) {
        final String dynamoDbColumn = encodeKeyBuffer(column);

        if (expectedValueMap.containsKey(dynamoDbColumn)) {
            return;
        }

        if (transaction.contains(store, key, column)) {
            final StaticBuffer expectedValue = transaction.get(store, key, column);
            final ExpectedAttributeValue expectedAttributeValue;
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
