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

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.thinkaurelius.titan.diskstorage.BackendException;

public class ListTablesWorker extends PaginatingTask<ListTablesRequest, ListTablesResult>
{
    private final ListTablesRequest request;
    private int returnedCount;
    private int scannedCount;
    private List<String> tableNames;
    private boolean hasNext;

    public ListTablesWorker(final DynamoDBDelegate delegate) {
        super(delegate, delegate.getListTablesApiName(), null /*tableName*/);
        this.request = new ListTablesRequest();
        this.returnedCount = 0;
        this.scannedCount = 0;
        this.tableNames = new ArrayList<>();
        this.hasNext = true;
    }

    @Override
    public ListTablesResult next() throws BackendException
    {
        final ListTablesResult result = delegate.listTables(request);
        if(result.getLastEvaluatedTableName() != null && !result.getLastEvaluatedTableName().isEmpty()) {
            request.setExclusiveStartTableName(result.getLastEvaluatedTableName());
        } else { //done
            markComplete();
        }

        // c add scanned items
        tableNames.addAll(result.getTableNames());
        return result;
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    public int getScannedCount() {
        return scannedCount;
    }

    public int getReturnedCount() {
        return returnedCount;
    }

    public ListTablesRequest getRequest() {
        return request;
    }

    protected void markComplete() {
        hasNext = false;
    }

    @Override
    protected ListTablesResult getMergedPages() {
        return new ListTablesResult().withTableNames(tableNames);
    }
}
