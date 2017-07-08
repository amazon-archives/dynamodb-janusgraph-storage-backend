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
package com.amazon.janusgraph.diskstorage.dynamodb.iterator;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.util.RecordIterator;

import com.amazon.janusgraph.diskstorage.dynamodb.Constants;
import com.amazon.janusgraph.diskstorage.dynamodb.DynamoDbStore;
import com.amazon.janusgraph.diskstorage.dynamodb.QueryWorker;
import com.amazon.janusgraph.diskstorage.dynamodb.builder.EntryBuilder;
import com.amazon.janusgraph.diskstorage.dynamodb.builder.KeyBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;

/**
 * Interprets Scan results for MULTI stores and assumes that results are SEQUENTIAL. This means that the scan is assumed to be non-segmented.
 * We need this assumption because it makes it so we don't need to keep track of where segment boundaries lie in order to avoid returning duplicate
 * hash keys.
 */
public class MultiRowSequentialScanInterpreter implements ScanContextInterpreter {

    private final DynamoDbStore store;
    private final SliceQuery sliceQuery;

    public MultiRowSequentialScanInterpreter(final DynamoDbStore store, final SliceQuery sliceQuery) {
        this.store = store;
        this.sliceQuery = sliceQuery;
    }

    @Override
    public List<SingleKeyRecordIterator> buildRecordIterators(final ScanContext scanContext) {
        final Map<String, AttributeValue> previousScanEnd = scanContext.getScanRequest().getExclusiveStartKey();

        // If there was a previous request, we can assume we already returned a RecordIterator for the last hash key in the previous request
        StaticBuffer previousKey = null;
        if (previousScanEnd != null && !previousScanEnd.isEmpty()) {
            previousKey = new KeyBuilder(previousScanEnd).build(Constants.JANUSGRAPH_HASH_KEY);
        }

        final List<Map<String, AttributeValue>> items = scanContext.getScanResult().getItems();
        final List<SingleKeyRecordIterator> recordIterators = Lists.newLinkedList();

        final Iterator<Map<String, AttributeValue>> itemIterator = items.iterator();
        while (itemIterator.hasNext()) {
            final Optional<Map<String, AttributeValue>> nextItem = findItemWithDifferentHashKey(itemIterator, previousKey);
            if (nextItem.isPresent()) {
                final Map<String, AttributeValue> item = nextItem.get();
                final StaticBuffer hashKey = new KeyBuilder(item).build(Constants.JANUSGRAPH_HASH_KEY);

                final Entry columnValue = new EntryBuilder(item).slice(sliceQuery.getSliceStart(),
                                                                       sliceQuery.getSliceEnd())
                                                                .build();
                // If the range key value is equal to the upper bound of the slice query, columnValue will be null
                // in these cases, we should not include the key in our list of iterators, because DynamoDB's upper bound
                // is inclusive, but Titan's upper bound is exclusive
                if (columnValue != null) {
                    recordIterators.add(new SingleKeyRecordIterator(hashKey, buildRecordIteratorForHashKey(hashKey)));
                }
                // It's always okay to move on to the next key after finding a single result, because the columns for each hash key are
                // scanned in order.
                previousKey = hashKey;
            }
        }

        return recordIterators;
    }

    private Optional<Map<String, AttributeValue>> findItemWithDifferentHashKey(final Iterator<Map<String, AttributeValue>> itemIterator, final StaticBuffer previousKey) {
        Optional<Map<String, AttributeValue>> result = Optional.absent();

        while (itemIterator.hasNext() && !result.isPresent()) {
            final Map<String, AttributeValue> item = itemIterator.next();
            final StaticBuffer nextKey = new KeyBuilder(item).build(Constants.JANUSGRAPH_HASH_KEY);
            if (!nextKey.equals(previousKey)) {
                result = Optional.of(item);
            }
        }
        return result;
    }

    private RecordIterator<Entry> buildRecordIteratorForHashKey(final StaticBuffer hashKey) {
        final QueryWorker queryWorker = store.buildQueryWorker(hashKey, sliceQuery);
        return new MultiRecordIterator(queryWorker, sliceQuery);
    }
}
