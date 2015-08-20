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
package com.amazon.titan.diskstorage.dynamodb.builder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.amazon.titan.diskstorage.dynamodb.Constants;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StaticBufferEntry;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;

/**
 * EntryBuilder is responsible for translating from DynamoDB item maps to Entry
 * objects.
 *
 * @author Matthew Sowders
 * @author Alexander Patrikalakis
 */
public class EntryBuilder extends AbstractBuilder {
    private final Map<String, AttributeValue> item;
    private StaticBuffer start;
    private StaticBuffer end;
    private boolean slice;
    private int limit = Integer.MAX_VALUE;

    public EntryBuilder(Map<String, AttributeValue> item) {
        this.item = item;
        item.remove(Constants.TITAN_HASH_KEY);
    }

    public List<Entry> buildAll() {
        if (null == item) {
            return Collections.emptyList();
        }
        List<Entry> filteredEntries = new ArrayList<>(item.size());
        final Entry sliceStartEntry = slice ? new StaticBufferEntry(start, ByteBufferUtil.emptyBuffer()) : null;
        final Entry sliceEndEntry = slice ? new StaticBufferEntry(end, ByteBufferUtil.emptyBuffer()) : null;
        for (String column : item.keySet()) {
            StaticBuffer columnKey = decodeKey(column);
            AttributeValue valueValue = item.get(column);
            StaticBuffer value = decodeValue(valueValue);
            final Entry entry = StaticBufferEntry.of(columnKey, value);
            if(!slice || (entry.compareTo(sliceStartEntry) >= 0 && entry.compareTo(sliceEndEntry) < 0)) {
                filteredEntries.add(StaticBufferEntry.of(columnKey, value));
            }
        }

        Collections.sort(filteredEntries);
        return filteredEntries.subList(0, Math.min(filteredEntries.size(), limit));
    }

    public Entry build() {
        if (null == item) {
            return null;
        }

        StaticBuffer rangeKey = decodeKey(item, Constants.TITAN_RANGE_KEY);
        return build(rangeKey);
    }

    public Entry build(StaticBuffer column) {
        if (null == item || null == column) {
            return null;
        }

        final AttributeValue valueValue = item.get(Constants.TITAN_VALUE);
        final StaticBuffer value = decodeValue(valueValue);

        // DynamoDB's between semantics include the end of a slice, but Titan expects the end to be exclusive
        if(slice && column.compareTo(end) == 0) {
            return null;
        }

        return new StaticBufferEntry(column, value);
    }

    public EntryBuilder slice(StaticBuffer start, StaticBuffer end) {
        this.start = start;
        this.end = end;
        this.slice = true;
        return this;
    }

    public EntryBuilder limit(int limit) {
        this.limit = limit;
        return this;
    }

}
