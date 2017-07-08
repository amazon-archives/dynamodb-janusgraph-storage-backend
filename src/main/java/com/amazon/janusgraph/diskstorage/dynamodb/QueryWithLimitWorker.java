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
package com.amazon.janusgraph.diskstorage.dynamodb;

import java.util.List;
import java.util.Map;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.StaticBuffer;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * QueryWorker that also enforces a limit on the total number of results it gathers before stopping. The number of items returned by this worker
 * will always be less than or equal to the given limit.
 */
public class QueryWithLimitWorker extends QueryWorker {

    private final int limit;

    QueryWithLimitWorker(final DynamoDbDelegate delegate, final QueryRequest request, final StaticBuffer titanKey, final int limit) {
        super(delegate, request, titanKey);
        this.limit = limit;
        request.setLimit(limit);
    }

    @Override
    public QueryResultWrapper next() throws BackendException {
        final QueryResultWrapper wrapper = super.next();

        final int returnedCount = getReturnedCount();
        // If we already have reached the limit for this query, we can stop making new requests
        if (returnedCount >= limit) {
            markComplete();
        } else {
            // Make sure we don't ask DynamoDB for more results than we care about
            final int maxRemainingRecords = limit - returnedCount;
            Preconditions.checkState(maxRemainingRecords > 0);
            getRequest().setLimit(maxRemainingRecords);
        }
        return wrapper;
    }

    @Override
    protected List<Map<String, AttributeValue>> getFinalItemList() {
        final Iterable<Map<String, AttributeValue>> limitedIter = Iterables.limit(super.getFinalItemList(), limit);
        return Lists.newArrayList(limitedIter);
    }
}
