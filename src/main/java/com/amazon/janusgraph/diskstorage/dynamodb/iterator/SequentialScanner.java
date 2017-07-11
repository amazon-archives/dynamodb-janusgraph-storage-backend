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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.amazon.janusgraph.diskstorage.dynamodb.BackendRuntimeException;
import com.amazon.janusgraph.diskstorage.dynamodb.DynamoDbDelegate;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.google.common.base.Preconditions;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Iterator that goes through a scan operation over a given DynamoDB table.
 * If a call to next() returns a result that indicates there are more records to scan, the next ScanRequest
 * is initiated asynchronously.
 *
 * @author Alexander Patrikalakis
 * @author Michael Rodaitis
 */
public class SequentialScanner implements Scanner {

    // This worker defaults to having a next result
    private boolean hasNext = true;

    private final DynamoDbDelegate dynamoDbDelegate;
    private final ScanRequest request;
    private int lastConsumedCapacity;
    private Future<ScanResult> currentFuture;

    public SequentialScanner(final DynamoDbDelegate dynamoDbDelegate, final ScanRequest request) {
        this.dynamoDbDelegate = dynamoDbDelegate;
        Preconditions.checkArgument(request.getExclusiveStartKey() == null || request.getExclusiveStartKey().isEmpty(),
                                    "A scan worker should start with a fresh ScanRequest");
        this.request = DynamoDbDelegate.copyScanRequest(request);
        this.lastConsumedCapacity = dynamoDbDelegate.estimateCapacityUnits(DynamoDbDelegate.SCAN, request.getTableName());
        this.currentFuture = dynamoDbDelegate.scanAsync(request, lastConsumedCapacity);
    }

    @Override
    public boolean hasNext() {
        return this.hasNext;
    }

    @SuppressFBWarnings(value = "IT_NO_SUCH_ELEMENT",
        justification = "https://github.com/awslabs/dynamodb-janusgraph-storage-backend/issues/222")
    @Override
    public ScanContext next() {
        ScanResult result = null;
        final boolean interrupted = false;
        try {
            result = currentFuture.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new BackendRuntimeException(dynamoDbDelegate.unwrapExecutionException(e, DynamoDbDelegate.SCAN));
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }

        // Copy the request used to get this ScanResult to create the proper ScanContext later
        final ScanRequest requestForResult = DynamoDbDelegate.copyScanRequest(this.request);

        if (result.getConsumedCapacity() != null) {
            lastConsumedCapacity = result.getConsumedCapacity().getCapacityUnits().intValue();
        }

        if (result.getLastEvaluatedKey() != null && !result.getLastEvaluatedKey().isEmpty()) {
            hasNext = true;
            request.setExclusiveStartKey(result.getLastEvaluatedKey());
            currentFuture = dynamoDbDelegate.scanAsync(request, lastConsumedCapacity);
        } else {
            hasNext = false;
        }
        return new ScanContext(requestForResult, result);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove");
    }

    @Override
    public void close() throws IOException {
        currentFuture.cancel(true);
    }
}
