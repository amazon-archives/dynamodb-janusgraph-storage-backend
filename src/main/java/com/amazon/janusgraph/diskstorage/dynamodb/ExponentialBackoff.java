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

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.TemporaryBackendException;

import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;

/**
 * A wrapper for a client-side exponential backoff retry strategy for DynamoDB API calls
 * @author Alexander Patrikalakis
 *
 * @param <RequestType> the type of AWS request that is input
 * @param <ResultType> the type of AWS request that is returned
 *
 */
public abstract class ExponentialBackoff<RequestType, ResultType> {
    public static final String RETRIES = "Retries";
    public static final String SCAN_RETRIES = DynamoDBDelegate.SCAN + RETRIES;
    public static final String QUERY_RETRIES = DynamoDBDelegate.QUERY + RETRIES;
    public static final String UPDATE_ITEM_RETRIES = DynamoDBDelegate.UPDATE_ITEM + RETRIES;
    public static final String DELETE_ITEM_RETRIES = DynamoDBDelegate.DELETE_ITEM + RETRIES;
    public static final String GET_ITEM_RETRIES = DynamoDBDelegate.GET_ITEM + RETRIES;

    public static final class Scan extends ExponentialBackoff<ScanRequest, ScanResult> {
        private final int permits;
        public Scan(ScanRequest request, DynamoDBDelegate delegate, int permits) {
            super(request, delegate, SCAN_RETRIES);
            this.permits = permits;
        }
        @Override
        protected ScanResult call() throws BackendException
        {
            return delegate.scan(request, permits);
        }
        @Override
        protected String getTableName()
        {
            return request.getTableName();
        }

    }

    public static final class Query extends ExponentialBackoff<QueryRequest, QueryResult> {
        private final int permits;
        public Query(QueryRequest request, DynamoDBDelegate delegate, int permits) {
            super(request, delegate, QUERY_RETRIES);
            this.permits = permits;
        }
        @Override
        protected QueryResult call() throws BackendException
        {
            return delegate.query(request, permits);
        }
        @Override
        protected String getTableName()
        {
            return request.getTableName();
        }

    }

    public static final class UpdateItem extends ExponentialBackoff<UpdateItemRequest, UpdateItemResult> {
        public UpdateItem(UpdateItemRequest request, DynamoDBDelegate delegate) {
            super(request, delegate, UPDATE_ITEM_RETRIES);
        }
        @Override
        protected UpdateItemResult call() throws BackendException
        {
            return delegate.updateItem(request);
        }
        @Override
        protected String getTableName()
        {
            return request.getTableName();
        }

    }

    public static final class DeleteItem extends ExponentialBackoff<DeleteItemRequest, DeleteItemResult> {
        public DeleteItem(DeleteItemRequest request, DynamoDBDelegate delegate) {
            super(request, delegate, DELETE_ITEM_RETRIES);
        }
        @Override
        protected DeleteItemResult call() throws BackendException
        {
            return delegate.deleteItem(request);
        }
        @Override
        protected String getTableName()
        {
            return request.getTableName();
        }

    }

    public static final class GetItem extends ExponentialBackoff<GetItemRequest, GetItemResult> {
        public GetItem(GetItemRequest request, DynamoDBDelegate delegate) {
            super(request, delegate, GET_ITEM_RETRIES);
        }
        @Override
        protected GetItemResult call() throws BackendException
        {
            return delegate.getItem(request);
        }
        @Override
        protected String getTableName()
        {
            return request.getTableName();
        }

    }

    private long exponentialBackoffTime;
    private long tries;
    private final String apiNameRetries;
    protected final RequestType request;
    protected ResultType result;
    protected final DynamoDBDelegate delegate;
    protected ExponentialBackoff(RequestType requestType, DynamoDBDelegate delegate, String apiNameTries) {
        this.request = requestType;
        this.delegate = delegate;
        this.exponentialBackoffTime = delegate.getRetryMillis();
        this.result = null;
        this.tries = 0;
        this.apiNameRetries = apiNameTries;
    }
    protected abstract ResultType call() throws BackendException;
    protected abstract String getTableName();

    public ResultType runWithBackoff() throws BackendException {
        boolean interrupted = false;
        try {
            do {
                tries++;
                try {
                    result = call();
                } catch(TemporaryBackendException e) { //retriable
                    if(tries > delegate.getMaxRetries()) {
                        throw new TemporaryBackendException("Max tries exceeded.", e);
                    }
                    try {
                        Thread.sleep(exponentialBackoffTime);
                    } catch(InterruptedException ie) {
                        interrupted = true;
                    } finally {
                        exponentialBackoffTime *= 2;
                    }
                    continue;
                }
            } while(result == null);
            return result;
        } finally {
            //meter tries
            delegate.getMeter(delegate.getMeterName(apiNameRetries, getTableName())).mark(tries - 1);

            if(interrupted) {
                Thread.currentThread().interrupt();
                throw new BackendRuntimeException("exponential backoff was interrupted");
            }
        }
    }
}
