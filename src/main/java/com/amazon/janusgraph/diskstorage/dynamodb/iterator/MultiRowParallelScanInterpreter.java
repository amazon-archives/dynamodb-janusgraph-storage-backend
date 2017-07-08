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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.util.RecordIterator;

import com.amazon.janusgraph.diskstorage.dynamodb.Constants;
import com.amazon.janusgraph.diskstorage.dynamodb.DynamoDbStore;
import com.amazon.janusgraph.diskstorage.dynamodb.QueryWorker;
import com.amazon.janusgraph.diskstorage.dynamodb.builder.KeyBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

/**
 * Builds RecordIterators for scan results from a parallel, segmented scan. To do this,
 * this class tracks the current "boundary" keys of each segment. That way, it can avoid returning duplicate keys
 * if a hash key spans multiple segments
 *
 * @author Matthew Sowders
 * @author Alexander Patrikalakis
 * @author Michael Rodaitis
 */
@RequiredArgsConstructor
public class MultiRowParallelScanInterpreter implements ScanContextInterpreter {

    private final Map<Integer, BoundaryKeys> segmentBoundaries = Maps.newHashMap();
    private final DynamoDbStore store;
    private final SliceQuery sliceQuery;

    /**
     * This class relies heavily on the behavior of segmented scans with respect to which hash keys are scanned by each segment.
     * Here's a rough ASCII example to help illustrate:
     *  ___________________________
     * |hk:A         |hk:B         |
     * ----------------------------
     * ^segment 1        ^segment 2
     *
     * Because we are scanning in segments across the entire hash key space, it is possible for the same hash key to appear in two different segments.
     * We are also running all of the scan segments in parallel, so we have no control over which segment returns first.
     *
     * In the example, if segment 2 was the first segment to post a result, we would store hk:B as a "boundary" key. That way when
     * segment 1 eventually reaches hk:B in its scan, we know that another segment has already returned this hash key and we can safely skip returning it.
     *
     * By doing this, we avoid returning a RecordIterator for the same hash key twice and we only need to store at most 2 hash keys per segment.
     *
     */
    @Override
    public List<SingleKeyRecordIterator> buildRecordIterators(final ScanContext scanContext) {
        final ScanResult dynamoDbResult = scanContext.getScanResult();
        final int segment = scanContext.getScanRequest().getSegment();
        final List<Map<String, AttributeValue>> items = dynamoDbResult.getItems();
        // If the scan returned no results, we need to shortcut and just throw back an empty result set
        if (items.isEmpty()) {
            return Collections.emptyList();
        }

        final List<SingleKeyRecordIterator> recordIterators = Lists.newLinkedList();

        final Iterator<Map<String, AttributeValue>> itemIterator = items.iterator();
        final Map<String, AttributeValue> firstItem = itemIterator.next();
        final StaticBuffer firstKey = new KeyBuilder(firstItem).build(Constants.JANUSGRAPH_HASH_KEY);

        // Computes the full set of boundary keys up to this point. This includes the previous end key for this segment.
        final ImmutableSet<StaticBuffer> boundaryKeys = aggregateBoundaryKeys();

        // The first key in this scan segment might already have been returned by a previous scan segment
        if (!boundaryKeys.contains(firstKey)) {
            recordIterators.add(buildRecordIteratorForHashKey(firstKey));
        }

        StaticBuffer hashKey = firstKey;
        while (itemIterator.hasNext()) {
            final Optional<StaticBuffer> nextKey = findNextHashKey(itemIterator, hashKey);
            if (nextKey.isPresent()) {
                // Found a new hash key. Make a record iterator and look for the next unique hash key
                hashKey = nextKey.get();
                recordIterators.add(buildRecordIteratorForHashKey(hashKey));
            }
        }

        // If we've already seen the final hashKey in a previous scan segment result, we want to avoid returning it again.
        if (!hashKey.equals(firstKey) && boundaryKeys.contains(hashKey)) {
            recordIterators.remove(recordIterators.size() - 1);
        }

        // Update the boundary keys for this segment
        if (scanContext.isFirstResult()) {
            setInitialBoundaryKeys(segment, firstKey, hashKey);
        } else {
            updateLastKey(segment, hashKey);
        }
        return recordIterators;
    }

    private Optional<StaticBuffer> findNextHashKey(final Iterator<Map<String, AttributeValue>> itemIterator, final StaticBuffer previousKey) {
        Optional<StaticBuffer> result = Optional.empty();

        while (itemIterator.hasNext() && !result.isPresent()) {
            final StaticBuffer nextKey = new KeyBuilder(itemIterator.next()).build(Constants.JANUSGRAPH_HASH_KEY);
            if (!nextKey.equals(previousKey)) {
                result = Optional.of(nextKey);
            }
        }

        return result;
    }

    private ImmutableSet<StaticBuffer> aggregateBoundaryKeys() {
        final Set<StaticBuffer> allBoundaryKeys = Sets.newHashSet();

        for (BoundaryKeys segmentBoundaryKeys : segmentBoundaries.values()) {
            allBoundaryKeys.add(segmentBoundaryKeys.firstKey);
            allBoundaryKeys.add(segmentBoundaryKeys.lastKey);
        }

        return ImmutableSet.copyOf(allBoundaryKeys);
    }

    private void setInitialBoundaryKeys(final int segment, final StaticBuffer firstKey, final StaticBuffer lastKey) {
        segmentBoundaries.put(segment, new BoundaryKeys(firstKey, lastKey));
    }

    private void updateLastKey(final int segment, final StaticBuffer lastKey) {
        segmentBoundaries.get(segment).setLastKey(lastKey);
    }

    private SingleKeyRecordIterator buildRecordIteratorForHashKey(final StaticBuffer hashKey) {
        final QueryWorker queryWorker = store.buildQueryWorker(hashKey, sliceQuery);
        final RecordIterator<Entry> columnIterator = new MultiRecordIterator(queryWorker, sliceQuery);
        return new SingleKeyRecordIterator(hashKey, columnIterator);
    }

    @AllArgsConstructor(access = AccessLevel.PACKAGE)
    @Getter(AccessLevel.PACKAGE)
    @Setter(AccessLevel.PACKAGE)
    private static class BoundaryKeys {
        private StaticBuffer firstKey;
        private StaticBuffer lastKey;
    }
}
