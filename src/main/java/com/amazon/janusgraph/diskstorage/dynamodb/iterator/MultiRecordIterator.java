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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.util.RecordIterator;

import com.amazon.janusgraph.diskstorage.dynamodb.QueryResultWrapper;
import com.amazon.janusgraph.diskstorage.dynamodb.QueryWorker;
import com.amazon.janusgraph.diskstorage.dynamodb.builder.EntryBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.google.common.collect.Lists;

/**
 * Lazy-loading iterator that pages through columns for a given hash key.
 * Used in conjunction with MultiRowKeyIteratorImpl to provide an implementation of getKeys(..) for the MULTI backend
 *
 * @author Michael Rodaitis
 */
public class MultiRecordIterator implements RecordIterator<Entry> {

    private StaticRecordIterator currentIterator;
    private final QueryWorker queryWorker;
    private final SliceQuery rangeKeySliceQuery;
    private boolean closed = false;

    public MultiRecordIterator(final QueryWorker queryWorker, final SliceQuery rangeKeySliceQuery) {
        this.queryWorker = queryWorker;
        this.rangeKeySliceQuery = rangeKeySliceQuery;
        this.currentIterator = new StaticRecordIterator(Collections.<Entry>emptyList());
    }

    @Override
    public void close() throws IOException {
        closed = true;
    }

    @Override
    public boolean hasNext() {
        if (closed) {
            return false;
        }
        if (currentIterator.hasNext()) {
            return true;
        }
        // Loop until the query finds a new result.
        // This is necessary because even if the query worker has a next page it might have no results.
        while (queryWorker.hasNext() && !currentIterator.hasNext()) {
            try {
                final QueryResultWrapper resultWrapper = queryWorker.next();
                final QueryResult queryResult = resultWrapper.getDynamoDBResult();

                currentIterator = buildRecordIteratorFromQueryResult(queryResult);
            } catch (BackendException e) {
                throw new RuntimeException(e);
            }
        }
        return currentIterator.hasNext();
    }

    private StaticRecordIterator buildRecordIteratorFromQueryResult(final QueryResult queryResult) {
        final List<Entry> entries = Lists.newLinkedList();
        for (Map<String, AttributeValue> item : queryResult.getItems()) {
            // DynamoDB's between includes the end of the range, but Titan's slice queries expect the end key to be exclusive
            final Entry entry = new EntryBuilder(item).slice(rangeKeySliceQuery.getSliceStart(), rangeKeySliceQuery.getSliceEnd())
                                                      .build();
            if (entry != null) {
                entries.add(entry);
            }
        }
        return new StaticRecordIterator(entries);
    }

    @Override
    public Entry next() {
        return currentIterator.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
