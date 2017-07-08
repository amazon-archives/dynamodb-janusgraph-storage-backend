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

import java.util.concurrent.Callable;

import org.janusgraph.diskstorage.BackendException;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

/**
 * Paginator base class for scan and query workers.
 * @param <R> the result type of a page of data.
 *
 * @author Alexander Patrikalakis
 *
 */
@RequiredArgsConstructor
public abstract class PaginatingTask<R> implements Callable<R> {
    protected final DynamoDbDelegate delegate;
    @Getter
    @Accessors(fluent = true)
    protected boolean hasNext = true;

    private int pagesProcessed = 0;
    private final String apiName;
    private final String tableName;

    /**
     * Paginates through pages of an entity.
     * @return an encapsulation of the pages of an entity
     * @throws BackendException if there was any trouble paginating
     */
    public R call() throws BackendException {
        while (hasNext) {
            pagesProcessed++;
            next();
        }
        delegate.updatePagesHistogram(apiName, tableName, pagesProcessed);
        return getMergedPages();
    }

    /**
     * Merges all the pages iterated through in this instance and returns them.
     * @return a merged view of all the paged results of this iterator
     */
    protected abstract R getMergedPages();

    /**
     * Moves the iterator to the next page in preparation for another hasNext/processPage call.
     * @return the next page
     * @throws BackendException if there was an exception thrown by the backend.
     */
    public abstract R next() throws BackendException;

    protected void markComplete() {
        hasNext = false;
    }
}
