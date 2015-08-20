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

import java.util.concurrent.Callable;

import com.thinkaurelius.titan.diskstorage.BackendException;

/**
 * Paginator base class for scan and query workers.
 *
 * @author Alexander Patrikalakis
 *
 */
public abstract class PaginatingTask<RequestType, ResultType> implements Callable<ResultType> {
    private int pagesProcessed;
    protected final DynamoDBDelegate delegate;
    private final String apiName;
    private final String tableName;
    protected PaginatingTask(DynamoDBDelegate delegate, String apiName, String tableName) {
        this.pagesProcessed = 0;
        this.delegate = delegate;
        this.apiName = apiName;
        this.tableName = tableName;
    }
    public ResultType call() throws BackendException
    {
        while(hasNext()) {
            pagesProcessed++;
            next();
        }
        delegate.updatePagesHistogram(apiName, tableName, pagesProcessed);
        return getMergedPages();
    }

    public int getPagesProcessed() {
        return pagesProcessed;
    }

    /**
     * Merges all the pages iterated through in this instance and returns them
     * @return a merged view of all the paged results of this iterator
     */
    protected abstract ResultType getMergedPages();

    /**
     * @return true if there are more pages to process and false if done
     */
    public abstract boolean hasNext();

    /**
     * moves the iterator to the next page in preparation for another hasNext/processPage call
     */
    public abstract ResultType next() throws BackendException;
}
