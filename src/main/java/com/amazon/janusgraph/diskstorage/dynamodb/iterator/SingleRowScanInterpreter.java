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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StaticArrayEntry;

import com.amazon.janusgraph.diskstorage.dynamodb.Constants;
import com.amazon.janusgraph.diskstorage.dynamodb.builder.EntryBuilder;
import com.amazon.janusgraph.diskstorage.dynamodb.builder.KeyBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.collect.Lists;

/**
 * Turns Scan results into RecordIterators for stores using the SINGLE data model.
 * This interpreter doesn't need to consider whether or not a scan is segmented,
 * because each item in a ScanResult represents ALL columns for a given key. It is impossible for
 * keys to be split across multiple ScanResults (or segments for that matter) when using the SINGLE data model.
 *
 * @author Michael Rodaitis
 */
public class SingleRowScanInterpreter implements ScanContextInterpreter {

    private final SliceQuery sliceQuery;

    public SingleRowScanInterpreter(final SliceQuery sliceQuery) {
        this.sliceQuery = sliceQuery;
    }

    @Override
    public List<SingleKeyRecordIterator> buildRecordIterators(final ScanContext scanContext) {
        final List<SingleKeyRecordIterator> recordIterators = Lists.newLinkedList();

        for (Map<String, AttributeValue> item : scanContext.getScanResult().getItems()) {
            final StaticBuffer hashKey = new KeyBuilder(item).build(Constants.JANUSGRAPH_HASH_KEY);
            final RecordIterator<Entry> recordIterator = createRecordIterator(item);
            if (recordIterator.hasNext()) {
                recordIterators.add(new SingleKeyRecordIterator(hashKey, recordIterator));
            }
        }

        return recordIterators;
    }

    private RecordIterator<Entry> createRecordIterator(final Map<String, AttributeValue> item) {
        item.remove(Constants.JANUSGRAPH_HASH_KEY);
        final List<Entry> entries = decodeSlice(item);
        final RecordIterator<Entry> iterator = new StaticRecordIterator(entries);
        return iterator;
    }

    private List<Entry> decodeSlice(final Map<String, AttributeValue> item) {
        final List<Entry> entries = new EntryBuilder(item).buildAll();
        final Entry sliceStartEntry = StaticArrayEntry.of(sliceQuery.getSliceStart(), BufferUtil.emptyBuffer());
        final Entry sliceEndEntry = StaticArrayEntry.of(sliceQuery.getSliceEnd(), BufferUtil.emptyBuffer());
        final List<Entry> filteredEntries = new ArrayList<>(entries.size());
        for (Entry entry : entries) {
            if (entry.compareTo(sliceStartEntry) >= 0 && entry.compareTo(sliceEndEntry) < 0) {
                filteredEntries.add(entry);
            }
        }
        return filteredEntries.subList(0, Math.min(filteredEntries.size(), sliceQuery.getLimit()));
    }

}
