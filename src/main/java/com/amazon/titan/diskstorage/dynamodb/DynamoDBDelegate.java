/*
 * Copyright 2014-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.util.AwsHostNameUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;

import com.amazon.titan.diskstorage.dynamodb.ExponentialBackoff.Scan;
import com.amazon.titan.diskstorage.dynamodb.iterator.ParallelScanner;
import com.amazon.titan.diskstorage.dynamodb.iterator.ScanSegmentWorker;
import com.amazon.titan.diskstorage.dynamodb.mutation.MutateWorker;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.RateLimiter;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.PermanentBackendException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.TemporaryBackendException;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.locking.PermanentLockingException;
import com.thinkaurelius.titan.util.stats.MetricManager;

/**
 * A wrapper on top of the DynamoDB client API that self-throttles using metric-based and context-aware
 * estimates for all Read APIs and for DeleteItem, and that self-throttles using accurate upper-bound
 * estimates of item size for PutItem and UpdateItem. Has a thread pool and is able to do parallel
 * UpdateItem / DeleteItem, Query, and Scan calls.
 *
 * @author Alexander Patrikalakis
 *
 */
public class DynamoDBDelegate
{
    public static final String PAGES = "Pages";
    public static final String CREATE_TABLE = "CreateTable";
    public static final String DELETE_TABLE = "DeleteTable";
    public static final String CONNECTION_RESET = "Connection reset";
    public static final String MUTATE_ITEM = "MutateItem";
    public static final String SUBSCRIBER_THROUGHPUT_LIMIT = "Cannot increase provisioned throughput to more than 80,000 units per account";
    public static final String HASH_RANGE_KEY_SIZE_LIMIT = "Hash primary key values must be under 2048 bytes, and range primary key values must be under 1024 bytes";
    public static final String UPDATE_ITEM_SIZE_LIMIT = "Item size to update has exceeded the maximum allowed size";
    public static final String VALIDATION_EXCEPTION = "ValidationException";
    public static final String USER_AGENT = "x-amz-user-agent";
    public static final String TITAN_USER_AGENT = "dynamodb-titan-storage-backend_1.0.0";
    public static final String PUT_ITEM = "PutItem";
    public static final String UPDATE_ITEM = "UpdateItem";
    public static final String DELETE_ITEM = "DeleteItem";
    public static final String QUERY = "Query";
    public static final String BATCH_WRITE_ITEM = "BatchWriteItem";
    public static final String GET_ITEM = "GetItem";
    public static final String DESCRIBE_TABLE = "DescribeTable";
    public static final String SCAN = "Scan";

    public static final int ONE_KILOBYTE = 1024;
    private static final long CONTROL_PLANE_RETRY_DELAY_MS = 1000;
    private static final String LIST_TABLES = "ListTables";

    private final AmazonDynamoDB client;
    private static ThreadPoolExecutor clientThreadPool = null;
    private final Map<String, RateLimiter> readRateLimit;
    private final Map<String, RateLimiter> writeRateLimit;
    private final RateLimiter controlPlaneRateLimiter;
    private final int maxConcurrentUsers;
    private final long maxRetries;
    private final long retryMillis;
    private final boolean embedded = false;
    private final String listTablesApiName;
    private final String executorGaugeName;
    private final String metricsPrefix;

    public DynamoDBDelegate(String endpoint, AWSCredentialsProvider provider,
        ClientConfiguration clientConfig, Configuration titanConfig,
        Map<String, RateLimiter> readRateLimit, Map<String, RateLimiter> writeRateLimit,
        long maxRetries, long retryMillis, String prefix, String metricsPrefix,
        RateLimiter controlPlaneRateLimiter) {
        if(prefix == null) {
            throw new IllegalArgumentException("prefix must be set");
        }
        if(metricsPrefix == null || metricsPrefix.isEmpty()) {
            throw new IllegalArgumentException("metrics-prefix may not be null or empty");
        }
        this.metricsPrefix = metricsPrefix;
        executorGaugeName = String.format("%s.%s_executor-queue-size", this.metricsPrefix, prefix);
        if(clientThreadPool == null) {
            clientThreadPool = Client.getPoolFromNs(titanConfig);
        }
        if(!MetricManager.INSTANCE.getRegistry().getNames().contains(executorGaugeName)) {
            MetricManager.INSTANCE.getRegistry().register(executorGaugeName, new Gauge<Integer>() {
                @Override
                public Integer getValue()
                {
                    return clientThreadPool.getQueue().size();
                }
            });
        }

        client = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(provider)
                .withClientConfiguration(clientConfig)
                .withEndpointConfiguration(getEndpointConfiguration(endpoint))
            .build();
        this.readRateLimit = readRateLimit;
        this.writeRateLimit = writeRateLimit;
        this.controlPlaneRateLimiter = controlPlaneRateLimiter;
        this.maxConcurrentUsers = titanConfig.get(Constants.DYNAMODB_CLIENT_EXECUTOR_MAX_CONCURRENT_OPERATIONS);
        this.maxRetries = maxRetries;
        this.retryMillis = retryMillis;
        if(maxConcurrentUsers < 1) {
            throw new IllegalArgumentException("need at least one user otherwise wont make progress on scan");
        }
        this.listTablesApiName = String.format("%s_ListTables", prefix);
    }

    @VisibleForTesting
    static AwsClientBuilder.EndpointConfiguration getEndpointConfiguration(String endpoint) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(endpoint), "must provide an endpoint URL");
        final String regionParsedFromEndpoint = AwsHostNameUtils.parseRegion(endpoint, AmazonDynamoDB.ENDPOINT_PREFIX);
        if(Strings.isNullOrEmpty(regionParsedFromEndpoint)) {
            //for use with DynamoDB Local, any signing region will do
            //TODO externalize signing region into a LOCAL scope config
            return new AwsClientBuilder.EndpointConfiguration(endpoint, Regions.US_EAST_2.getName());
        }
        return new AwsClientBuilder.EndpointConfiguration(endpoint, regionParsedFromEndpoint);
    }

    @VisibleForTesting
    AmazonDynamoDB client() {
        return client;
    }

    private <T extends AmazonWebServiceRequest> T setUserAgent(T request) {
        request.putCustomRequestHeader(USER_AGENT, TITAN_USER_AGENT);
        return request;
    }

    public BackendException processDynamoDBAPIException(Throwable e, String apiName, String tableName) {
        Preconditions.checkArgument(apiName != null);
        Preconditions.checkArgument(!apiName.isEmpty());
        final String prefix = tableName == null ? apiName : String.format("%s_%s", apiName, tableName);
        final String message = String.format("%s %s", prefix, e.getMessage());
        if (e instanceof ResourceNotFoundException) {
            return new BackendNotFoundException(String.format("%s; table not found", message), e);
        } else if(e instanceof ConditionalCheckFailedException) {
            return new PermanentLockingException(message, e);
        } else if(e instanceof AmazonServiceException) {
            if(e.getMessage() != null &&
                (e.getMessage().contains(HASH_RANGE_KEY_SIZE_LIMIT) || e.getMessage().contains(UPDATE_ITEM_SIZE_LIMIT))) {
                return new PermanentBackendException(message, e);
            } else {
                return new TemporaryBackendException(message, e);
            }
        } else if(e instanceof AmazonClientException) { //all client exceptions are retriable by default
            return new TemporaryBackendException(message, e);
        } else if(e instanceof SocketException) { //sometimes this doesn't get caught by SDK
            return new TemporaryBackendException(message, e);
        }
        // unknown exception type
        return new PermanentBackendException(message, e);
    }

    public ScanResult scan(ScanRequest request, int permitsToConsume) throws BackendException {
        setUserAgent(request);
        ScanResult result;
        timedReadThrottle(SCAN, request.getTableName(), permitsToConsume);

        final Timer.Context apiTimerContext = getTimerContext(SCAN, request.getTableName());
        try {
            result = client.scan(request);
        } catch (Exception e) {
            throw processDynamoDBAPIException(e, SCAN, request.getTableName());
        } finally {
            apiTimerContext.stop();
        }
        meterConsumedCapacity(SCAN, result.getConsumedCapacity());
        measureItemCount(SCAN, request.getTableName(), result.getCount());
        return result;
    }

    public ParallelScanner getParallelScanCompletionService(ScanRequest initialRequest) throws BackendException {
        final int segments = Math.max(1, clientThreadPool.getMaximumPoolSize() / maxConcurrentUsers);
        ParallelScanner completion = new ParallelScanner(clientThreadPool, segments, this);

        for (int segment = 0; segment < segments; segment++) {
            // dont need to set user agent here because ExponentialBackoff.Scan
            // calls DynamoDBDelegate.scan which sets it
            ScanRequest scanSegment = copyScanRequest(initialRequest).withTotalSegments(segments).withSegment(segment);
            completion.addWorker(new ScanSegmentWorker(this, scanSegment), segment);
        }

        return completion;
    }


    public Future<ScanResult> scanAsync(final ScanRequest request, final int permitsToConsume) {
        return clientThreadPool.submit(new Callable<ScanResult>() {
            @Override
            public ScanResult call() throws Exception {
                final Scan backoff = new Scan(request, DynamoDBDelegate.this, permitsToConsume);
                return backoff.runWithBackoff();
            }
        });
    }

    public static ScanRequest copyScanRequest(ScanRequest request) {
        return new ScanRequest().withAttributesToGet(request.getAttributesToGet())
            .withScanFilter(request.getScanFilter())
            .withConditionalOperator(request.getConditionalOperator())
            .withExclusiveStartKey(request.getExclusiveStartKey())
            .withExpressionAttributeNames(request.getExpressionAttributeNames())
            .withExpressionAttributeValues(cloneItem(request.getExpressionAttributeValues()))
            .withFilterExpression(request.getFilterExpression())
            .withIndexName(request.getIndexName()).withLimit(request.getLimit())
            .withProjectionExpression(request.getProjectionExpression())
            .withReturnConsumedCapacity(request.getReturnConsumedCapacity())
            .withScanFilter(request.getScanFilter()).withSelect(request.getSelect())
            .withTableName(request.getTableName()).withTotalSegments(request.getTotalSegments())
            .withSegment(request.getSegment());
    }

    public void parallelMutate(List<MutateWorker> workers) throws BackendException {
        CompletionService<Void> completion = new ExecutorCompletionService<>(clientThreadPool);
        List<Future<Void>> futures = Lists.newLinkedList();
        for (MutateWorker worker : workers) {
            futures.add(completion.submit(worker));
        }

        //block on the futures all getting or throwing instead of using a latch as i need to check future status anyway
        boolean interrupted = false;
        try {
            for (int i = 0; i < workers.size(); i++) {
                try {
                    completion.take().get(); //Void
                } catch (InterruptedException e) {
                    interrupted = true;
                    // fail out because titan does not poll this thread for interrupted anywhere
                    throw new BackendRuntimeException("was interrupted during parallelMutate");
                } catch (ExecutionException e) {
                    throw unwrapExecutionException(e, MUTATE_ITEM);
                }
            }
        } finally {
            for (Future<Void> future : futures) {
                if (!future.isDone()) {
                    future.cancel(interrupted /* mayInterruptIfRunning */);
                }
            }
            if (interrupted) {
                // set interrupted on this thread
                Thread.currentThread().interrupt();
            }
        }
    }

    public List<QueryResultWrapper> parallelQuery(List<QueryWorker> queryWorkers) throws BackendException {
        CompletionService<QueryResultWrapper> completionService = new ExecutorCompletionService<>(clientThreadPool);

        List<Future<QueryResultWrapper>> futures = Lists.newLinkedList();
        for (QueryWorker worker : queryWorkers) {
            futures.add(completionService.submit(worker));
        }

        boolean interrupted = false;
        List<QueryResultWrapper> results = Lists.newLinkedList();
        try {
            for (int i = 0; i < queryWorkers.size(); i++) {
                try {
                    QueryResultWrapper result = completionService.take().get();
                    results.add(result);
                } catch (InterruptedException e) {
                    interrupted = true;
                    // fail out because titan does not poll this thread for interrupted anywhere
                    throw new BackendRuntimeException("was interrupted during parallelQuery");
                } catch (ExecutionException e) {
                    throw unwrapExecutionException(e, QUERY);
                }
            }
        } finally {
            for (Future<QueryResultWrapper> future : futures) {
                if (!future.isDone()) {
                    future.cancel(interrupted /* mayInterruptIfRunning */);
                }
            }

            if (interrupted) {
                // set interrupted on this thread and fail out
                Thread.currentThread().interrupt();
            }
        }
        return results;
    }

    public Map<StaticBuffer, GetItemResult> parallelGetItem(List<GetItemWorker> workers) throws BackendException {
        final CompletionService<GetItemResultWrapper> completionService = new ExecutorCompletionService<>(clientThreadPool);

        final List<Future<GetItemResultWrapper>> futures = Lists.newLinkedList();
        for (GetItemWorker worker : workers) {
            futures.add(completionService.submit(worker));
        }

        boolean interrupted = false;
        final Map<StaticBuffer, GetItemResult> results = Maps.newHashMap();
        try {
            for (int i = 0; i < workers.size(); i++) {
                try {
                    GetItemResultWrapper result = completionService.take().get();
                    results.put(result.getTitanKey(), result.getDynamoDBResult());
                } catch (InterruptedException e) {
                    interrupted = true;
                    throw new BackendRuntimeException("was interrupted during parallelGet");
                } catch (ExecutionException e) {
                    throw unwrapExecutionException(e, GET_ITEM);
                }
            }
        } finally {
            for (Future<GetItemResultWrapper> future : futures) {
                if (!future.isDone()) {
                    future.cancel(interrupted /* mayInterruptIfRunning */);
                }
            }

            if (interrupted) {
                // set interrupted on this thread and fail out
                Thread.currentThread().interrupt();
            }
        }
        return results;
    }

    public BackendException unwrapExecutionException(ExecutionException e, String apiName) {
        final Throwable cause = e.getCause();
        if (cause instanceof BackendException) {
            return (BackendException) cause; //already translated
        } else {
             //ok not to drill down to specific because would have thrown permanentbackend exception for other
            return processDynamoDBAPIException(cause, apiName, null /*tableName*/);
        }
    }

    public GetItemResult getItem(GetItemRequest request) throws BackendException {
        setUserAgent(request);
        GetItemResult result;
        timedReadThrottle(GET_ITEM, request.getTableName(), estimateCapacityUnits(GET_ITEM, request.getTableName()));
        final Timer.Context apiTimerContext = getTimerContext(GET_ITEM, request.getTableName());
        try {
            result = client.getItem(request);
        } catch (Exception e) {
            throw processDynamoDBAPIException(e, GET_ITEM, request.getTableName());
        } finally {
            apiTimerContext.stop();
        }
        meterConsumedCapacity(GET_ITEM, result.getConsumedCapacity());
        return result;
    }

    public BatchWriteItemResult batchWriteItem(BatchWriteItemRequest batchRequest) throws BackendException {
        int count = 0;
        for(Entry<String,java.util.List<WriteRequest>> entry : batchRequest.getRequestItems().entrySet()) {
            final String tableName = entry.getKey();
            final List<WriteRequest> requests = entry.getValue();
            count += requests.size();
            if(count > 25) {
                throw new IllegalArgumentException("cant have more than 25 requests in a batchwrite");
            }
            for(WriteRequest request : requests) {
                if(!(request.getPutRequest() != null ^ request.getDeleteRequest() != null)) {
                    throw new IllegalArgumentException("Exactly one of PutRequest or DeleteRequest must be set in each WriteRequest in a batch write operation");
                }
                final int wcu;
                final String apiName;
                if(request.getPutRequest() != null) {
                    apiName = PUT_ITEM;
                    final int bytes = calculateItemSizeInBytes(request.getPutRequest().getItem());
                    wcu = computeWcu(bytes);
                } else { //deleterequest
                    apiName = DELETE_ITEM;
                    wcu = estimateCapacityUnits(apiName, tableName);
                }
                timedWriteThrottle(apiName, tableName, wcu);
            }
        }

        BatchWriteItemResult result;
        setUserAgent(batchRequest);
        final Timer.Context apiTimerContext = getTimerContext(BATCH_WRITE_ITEM, null /*tableName*/);
        try {
            result = client.batchWriteItem(batchRequest);
        } catch (Exception e) {
            throw processDynamoDBAPIException(e, BATCH_WRITE_ITEM, null /*tableName*/);
        } finally {
            apiTimerContext.stop();
        }
        if(result.getConsumedCapacity() != null) {
            for(ConsumedCapacity ccu : result.getConsumedCapacity()) {
                meterConsumedCapacity(BATCH_WRITE_ITEM, ccu);
            }
        }
        return result;
    }

    public QueryResult query(QueryRequest request, int permitsToConsume) throws BackendException {
        setUserAgent(request);
        QueryResult result;
        timedReadThrottle(QUERY, request.getTableName(), permitsToConsume);
        final Timer.Context apiTimerContext = getTimerContext(QUERY, request.getTableName());
        try {
            result = client.query(request);
        } catch (Exception e) {
            throw processDynamoDBAPIException(e, QUERY, request.getTableName());
        } finally {
            apiTimerContext.stop();
        }
        meterConsumedCapacity(QUERY, result.getConsumedCapacity());
        measureItemCount(QUERY, request.getTableName(), result.getCount());
        return result;
    }

    public PutItemResult putItem(PutItemRequest request) throws BackendException {
        setUserAgent(request);
        PutItemResult result;
        final int bytes = calculateItemSizeInBytes(request.getItem());
        getBytesHistogram(PUT_ITEM, request.getTableName()).update(bytes);
        final int wcu = computeWcu(bytes);
        timedWriteThrottle(PUT_ITEM, request.getTableName(), wcu);

        final Timer.Context apiTimerContext = getTimerContext(PUT_ITEM, request.getTableName());
        try {
            result = client.putItem(request);
        } catch (Exception e) {
            throw processDynamoDBAPIException(e, PUT_ITEM, request.getTableName());
        } finally {
            apiTimerContext.stop();
        }
        meterConsumedCapacity(PUT_ITEM, result.getConsumedCapacity());

        return result;
    }

    public UpdateItemResult updateItem(UpdateItemRequest request) throws BackendException {
        setUserAgent(request);
        UpdateItemResult result;
        final int bytes = request.getUpdateExpression() != null ? calculateExpressionBasedUpdateSize(request) : calculateItemUpdateSizeInBytes(request.getAttributeUpdates());
        getBytesHistogram(UPDATE_ITEM, request.getTableName()).update(bytes);
        final int wcu = computeWcu(bytes);
        timedWriteThrottle(UPDATE_ITEM, request.getTableName(), wcu);

        final Timer.Context apiTimerContext = getTimerContext(UPDATE_ITEM, request.getTableName());
        try {
            result = client.updateItem(request);
        } catch (Exception e) {
            throw processDynamoDBAPIException(e, UPDATE_ITEM, request.getTableName());
        } finally {
            apiTimerContext.stop();
        }
        meterConsumedCapacity(UPDATE_ITEM, result.getConsumedCapacity());

        return result;
    }

    /**
     * This method calculates a lower bound of the size of a new item created with UpdateItem UpdateExpression. It does not
     * account for the size of the attribute names of the document paths in the attribute names map and it assumes that the
     * UpdateExpression only uses the SET action to assign to top-level attributes.
     * @param request UpdateItem request that uses update expressions
     * @return the size of the post-update image of the item
     */
    private int calculateExpressionBasedUpdateSize(UpdateItemRequest request)
    {
        if(request == null || request.getUpdateExpression() == null) {
            throw new IllegalArgumentException("request did not use update expression");
        }
        int size = calculateItemSizeInBytes(request.getKey());
        for(AttributeValue value : request.getExpressionAttributeValues().values()) {
            size += calculateAttributeSizeInBytes(value);
        }
        return size;
    }

    public DeleteItemResult deleteItem(DeleteItemRequest request) throws BackendException {
        setUserAgent(request);
        DeleteItemResult result;
        int wcu = estimateCapacityUnits(DELETE_ITEM, request.getTableName());
        timedWriteThrottle(DELETE_ITEM, request.getTableName(), wcu);

        final Timer.Context apiTimerContext = getTimerContext(DELETE_ITEM, request.getTableName());
        try {
            result = client.deleteItem(request);
        } catch (Exception e) {
            throw processDynamoDBAPIException(e, DELETE_ITEM, request.getTableName());
        } finally {
            apiTimerContext.stop();
        }
        meterConsumedCapacity(DELETE_ITEM, result.getConsumedCapacity());

        return result;
    }

    public int estimateCapacityUnits(String apiName, String tableName) {
        int cu = 1;
        final Meter apiCcuMeter = getConsumedCapacityMeter(apiName, tableName);
        final Timer apiTimer = getTimer(apiName, tableName);

        if(apiCcuMeter != null && apiTimer != null) {
            if(apiTimer.getCount() > 0) {
                cu = (int) Math.round(Math.max(1.0, (double) apiCcuMeter.getCount() / (double) apiTimer.getCount()));
            }
        }
        return cu;
    }

    public RateLimiter readRateLimit(String tableName) {
        return readRateLimit.get(tableName);
    }

    public RateLimiter writeRateLimit(String tableName) {
        return writeRateLimit.get(tableName);
    }

    private void timedWriteThrottle(String apiName, String tableName, int permits)
    {
        timedThrottle(apiName, writeRateLimit(tableName), tableName, permits);
    }

    private void timedReadThrottle(String apiName, String tableName, int permits)
    {
        timedThrottle(apiName, readRateLimit(tableName), tableName, permits);
    }

    private void timedThrottle(String apiName, RateLimiter limiter, String tableName, int permits)
    {
        if(limiter == null) {
            throw new IllegalArgumentException("limiter for " + apiName + " on table " + tableName + " was null");
        }
        final Timer.Context throttleTimerCtxt = getTimerContext(String.format("%sThrottling", apiName), tableName);
        try {
            limiter.acquire(permits);
        } finally {
            throttleTimerCtxt.stop();
        }
    }

    public ListTablesResult listTables(ListTablesRequest request) throws BackendException {
        controlPlaneRateLimiter.acquire();
        final Timer.Context apiTimerContext = getTimerContext(listTablesApiName, null /*tableName*/);
        ListTablesResult result;
        try {
            result = client.listTables(request);
        } catch (Exception e) {
            throw processDynamoDBAPIException(e, LIST_TABLES, null /*tableName*/);
        } finally {
            apiTimerContext.stop();
        }
        return result;
    }

    public ListTablesResult listAllTables() throws BackendException {
        ListTablesWorker worker = new ListTablesWorker(this);
        worker.call();
        return worker.getMergedPages();
    }

    public TableDescription describeTable(String tableName) throws BackendException {
        return describeTable(new DescribeTableRequest().withTableName(tableName)).getTable();
    }

    public DescribeTableResult describeTable(DescribeTableRequest request) throws BackendException {
        controlPlaneRateLimiter.acquire();
        final Timer.Context apiTimerContext = getTimerContext(DESCRIBE_TABLE, request.getTableName());
        DescribeTableResult result;
        try {
            result = client.describeTable(request);
        } catch (Exception e) {
            throw processDynamoDBAPIException(e, DESCRIBE_TABLE, request.getTableName());
        } finally {
            apiTimerContext.stop();
        }
        return result;
    }

    public DeleteTableResult deleteTable(DeleteTableRequest request) throws BackendException {
        controlPlaneRateLimiter.acquire();
        final Timer.Context apiTimerContext = getTimerContext(DELETE_TABLE, request.getTableName());
        DeleteTableResult result;
        try {
            result = client.deleteTable(request);
        } catch (Exception e) {
            throw processDynamoDBAPIException(e, DELETE_TABLE, request.getTableName());
        } finally {
            apiTimerContext.stop();
        }
        return result;
    }

    public void interruptibleSleep(long millis) {
        boolean interrupted = false;
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            interrupted = true;
        } finally {
            if(interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public boolean ensureTableDeleted(String tableName) throws BackendException {
        boolean successFlag = false;
        int retryCount = 0;
        do {
            try {
                this.describeTable(tableName);
            } catch (BackendNotFoundException e) {
                successFlag = true;
                break;
            }

            interruptibleSleep(CONTROL_PLANE_RETRY_DELAY_MS);
            retryCount++;
        } while(!successFlag && retryCount < maxRetries);
        if (!successFlag) {
            throw new PermanentBackendException("Table deletion not completed after retrying " + maxRetries + " times");
        }
        return successFlag;
    }

    public DeleteTableResult deleteTable(String tableName) throws BackendException {
        return deleteTable(new DeleteTableRequest().withTableName(tableName));
    }

    public CreateTableResult createTable(CreateTableRequest request) throws BackendException {
        controlPlaneRateLimiter.acquire();
        final Timer.Context apiTimerContext = getTimerContext(CREATE_TABLE, request.getTableName());
        CreateTableResult result;
        try {
            result = client.createTable(request);
        } catch (Exception e) {
            throw processDynamoDBAPIException(e, CREATE_TABLE, request.getTableName());
        } finally {
            apiTimerContext.stop();
        }
        return result;
    }

    private static boolean isTableAcceptingWrites(String status) {
        return isTableStatus(TableStatus.ACTIVE, status) || isTableStatus(TableStatus.UPDATING, status);
    }

    private static boolean isTableStatus(TableStatus constant, String status) {
        return constant.toString().equals(status);
    }

    public void waitForTableCreation(String tableName, boolean verifyIndexesList,
        List<LocalSecondaryIndexDescription> expectedLsiList, List<GlobalSecondaryIndexDescription> expectedGsiList) throws BackendException {
        boolean successFlag = false;
        int retryCount = 0;
        while (!successFlag && retryCount < maxRetries) {
            try {
                boolean areAllGSIsActive = true;
                final TableDescription td = describeTable(tableName);
                if (verifyIndexesList) {
                    Set<LocalSecondaryIndexDescription> expectedLSIs = new HashSet<LocalSecondaryIndexDescription>();
                    if (expectedLsiList != null) {
                        expectedLSIs.addAll(expectedLsiList);
                    }
                    Set<LocalSecondaryIndexDescription> actualLSIs = new HashSet<LocalSecondaryIndexDescription>();
                    if (td.getLocalSecondaryIndexes() != null) {
                        actualLSIs.addAll(td.getLocalSecondaryIndexes());
                    }
                    // the lsi list should be there even if the table is in creating state
                    if(!((expectedLsiList == null && td.getLocalSecondaryIndexes() == null) || expectedLSIs.equals(actualLSIs))) {
                        throw new PermanentBackendException("LSI list is not as expected during table creation. expectedLsiList=" +
                            expectedLsiList.toString() + "; table description=" + td.toString());
                    }

                    // ignore the status of all GSIs since they will mess up .equals()
                    if (td.getGlobalSecondaryIndexes() != null) {
                        for (GlobalSecondaryIndexDescription gDesc : td.getGlobalSecondaryIndexes()) {
                            if (!isTableAcceptingWrites(gDesc.getIndexStatus())) {
                                areAllGSIsActive = false;
                                break;
                            }
                        }
                    }

                    // the gsi list should be there even if the table is in creating state
                    if (!areGSIsSameConfiguration(expectedGsiList, td.getGlobalSecondaryIndexes())) {
                        throw new PermanentBackendException("GSI list is not as expected during table creation. expectedGsiList="
                            + expectedGsiList.toString() + "; table description=" + td.toString());
                    }
                }
                successFlag = isTableAcceptingWrites(td.getTableStatus()) && areAllGSIsActive;
            } catch (BackendNotFoundException e) {
                successFlag = false;
            }

            if (!successFlag) {
                interruptibleSleep(CONTROL_PLANE_RETRY_DELAY_MS);
            }
            retryCount++;
        }
        if (!successFlag) {
            throw new PermanentBackendException("Table creation not completed for table " + tableName + " after retrying "
                    + this.maxRetries + " times for a duration of " + CONTROL_PLANE_RETRY_DELAY_MS * this.maxRetries + " ms");
        }
    }

    public static boolean areGSIsSameConfiguration(List<GlobalSecondaryIndexDescription> g1,
            List<GlobalSecondaryIndexDescription> g2) {
        if (g1 == null) {
            if (g2 == null) {
                return true;
            }
            return false;
        }
        if(g1.size() != g2.size()) {
            return false;
        }
        // make copy of the lists because we don't want to mutate the lists
        ArrayList<GlobalSecondaryIndexDescription> g1clone = new ArrayList<>(g1.size());
        g1clone.addAll(g1);
        ArrayList<GlobalSecondaryIndexDescription> g2clone = new ArrayList<>(g2.size());
        g1clone.addAll(g2);

        for (GlobalSecondaryIndexDescription gi1 : g1) {
            for (GlobalSecondaryIndexDescription gi2 : g2) {
                if (areGSIsSameConfiguration(gi1, gi2)) {
                    g1clone.remove(gi1);
                    g2clone.remove(gi2);
                    break;
                }
            }
        }
        return g1clone.isEmpty() || g2clone.isEmpty();
    }

    public static boolean areGSIsSameConfiguration(GlobalSecondaryIndexDescription g1, GlobalSecondaryIndexDescription g2) {
        if (g1 == null ^ g2 == null) {
            return false;
        }
        if (g1 == g2) {
            return true;
        }
        final EqualsBuilder builder = new EqualsBuilder();
        builder.append(g1.getIndexName(), g2.getIndexName());
        builder.append(g1.getKeySchema(), g2.getKeySchema());
        builder.append(g1.getProjection().getProjectionType(), g2.getProjection().getProjectionType());
        builder.append(g1.getProvisionedThroughput().getReadCapacityUnits(), g2.getProvisionedThroughput().getReadCapacityUnits());
        builder.append(g1.getProvisionedThroughput().getWriteCapacityUnits(), g2.getProvisionedThroughput().getWriteCapacityUnits());

        final Set<String> projectionNonKeyAttributesG1 =
            g1.getProjection().getNonKeyAttributes() == null ? Collections.<String>emptySet() : new HashSet<String>(g1.getProjection().getNonKeyAttributes());
        final Set<String> projectionNonKeyAttributesG2 =
            g2.getProjection().getNonKeyAttributes() == null ? Collections.<String>emptySet() : new HashSet<String>(g2.getProjection().getNonKeyAttributes());
        builder.append(projectionNonKeyAttributesG1, projectionNonKeyAttributesG2);

        return builder.build().booleanValue();
    }

    public void createTableAndWaitForActive(CreateTableRequest request) throws BackendException {
        final String tableName = request.getTableName();
        TableDescription desc;
        try {
            desc = this.describeTable(tableName);
            if (null != desc) {
                if (isTableAcceptingWrites(desc.getTableStatus())) {
                    return; //store existed
                }
            }
        } catch (BackendNotFoundException e) {
            //Swallow, table doesnt exist yet
        }

        createTable(request);
        waitForTableCreation(tableName, false /*verifyIndexesList*/, null /*expectedLsiList*/, null /*expectedGsiList*/);
    }

    public void shutdown() {
        MetricManager.INSTANCE.getRegistry().remove(executorGaugeName);
        // TODO(amcp) figure out a way to make the thread pool not be static
        client.shutdown();
    }

    public final Timer getTimer(String apiName, String tableName) {
        return MetricManager.INSTANCE.getTimer(getMeterName(apiName, tableName));
    }
    public final Timer.Context getTimerContext(String apiName, String tableName) {
        return getTimer(apiName, tableName).time();
    }
    public final Meter getMeter(String meterName) {
        return MetricManager.INSTANCE.getRegistry().meter(meterName);
    }
    public final String getItemCountMeterName(String apiName, String tableName) {
        return getMeterName(String.format("%sItemCount", apiName), tableName);
    }
    public final void measureItemCount(String apiName, String tableName, long itemCount) {
        getMeter(getItemCountMeterName(apiName, tableName)).mark(itemCount);
        getCounter(apiName, tableName, "ItemCountCounter").inc(itemCount);
    }
    private Counter getCounter(String apiName, String tableName, String quantity) {
        return MetricManager.INSTANCE.getCounter(getQuantityName(apiName, tableName, quantity));
    }
    public final void meterConsumedCapacity(String apiName, ConsumedCapacity ccu) {
        if(ccu != null) {
            getConsumedCapacityMeter(apiName, ccu.getTableName()).mark(Math.round(ccu.getCapacityUnits()));
        }
    }
    public final String getQuantityName(String apiName, String tableName, String quantity) {
        return getMeterName(String.format("%s%s", apiName, quantity), tableName);
    }
    public final Meter getQuantityMeter(String apiName, String tableName, String quantity) {
        return getMeter(getQuantityName(apiName, tableName, quantity));
    }
    public final Meter getConsumedCapacityMeter(String apiName, String tableName) {
        return getQuantityMeter(apiName, tableName, "ConsumedCapacity");
    }
    public final Histogram getBytesHistogram(String apiName, String tableName) {
        return getHistogram(apiName, tableName, "Bytes");
    }
    public final Histogram getHistogram(String apiName, String tableName, String quantity) {
        return MetricManager.INSTANCE.getHistogram(getQuantityName(apiName, tableName, quantity));
    }
    public final Histogram getPagesHistogram(String apiName, String tableName) {
        return getHistogram(apiName, tableName, PAGES);
    }
    public final void updatePagesHistogram(String apiName, String tableName, int pagesProcessed) {
        getHistogram(apiName, tableName, PAGES).update(pagesProcessed);
    }
    public final String getMeterName(String apiName, String tableName) {
        return tableName == null ?
            String.format("%s.%s", metricsPrefix, apiName) :
            String.format("%s.%s.%s", metricsPrefix, apiName, tableName);
    }
    public final int getMaxConcurrentUsers() {
        return this.maxConcurrentUsers;
    }

    /**
     * Helper method that clones an item
     *
     * @param item the item to clone
     * @return a clone of item.
     */
    public static Map<String, AttributeValue> cloneItem(Map<String, AttributeValue> item) {
        if (item == null) {
            return null;
        }
        Map<String, AttributeValue> clonedItem = Maps.newHashMap();
        IdentityHashMap<AttributeValue, AttributeValue> sourceDestinationMap = new IdentityHashMap<>();

        for (Entry<String, AttributeValue> entry : item.entrySet()) {
            if (!sourceDestinationMap.containsKey(entry.getValue())) {
                sourceDestinationMap.put(entry.getValue(), clone(entry.getValue(), sourceDestinationMap));
            }
            clonedItem.put(entry.getKey(), sourceDestinationMap.get(entry.getValue()));
        }
        return clonedItem;
    }

    /**
     * Helper method that can clone an Attribute Value
     *
     * @param val the AttributeValue to copy
     * @param sourceDestinationMap used to avoid loops by keeping track of references
     * @return a copy of val
     */
    public static AttributeValue clone(AttributeValue val, IdentityHashMap<AttributeValue, AttributeValue> sourceDestinationMap) {
        if (val == null) {
            return null;
        }

        if (sourceDestinationMap.containsKey(val)) {
            return sourceDestinationMap.get(val);
        }

        AttributeValue clonedVal = new AttributeValue();
        sourceDestinationMap.put(val, clonedVal);
        if (val.getN() != null) {
            clonedVal.setN(val.getN());
        } else if (val.getS() != null) {
            clonedVal.setS(val.getS());
        } else if (val.getB() != null) {
            clonedVal.setB(val.getB());
        } else if (val.getNS() != null) {
            clonedVal.setNS(val.getNS());
        } else if (val.getSS() != null) {
            clonedVal.setSS(val.getSS());
        } else if (val.getBS() != null) {
            clonedVal.setBS(val.getBS());
        } else if (val.getBOOL() != null) {
            clonedVal.setBOOL(val.getBOOL());
        } else if (val.getNULL() != null) {
            clonedVal.setNULL(val.getNULL());
        } else if (val.getL() != null) {
            List<AttributeValue> list = new ArrayList<>(val.getL().size());
            for (AttributeValue listItemValue : val.getL()) {
                if (!sourceDestinationMap.containsKey(listItemValue)) {
                    sourceDestinationMap.put(listItemValue, clone(listItemValue, sourceDestinationMap));
                }
                list.add(sourceDestinationMap.get(listItemValue));
            }
            clonedVal.setL(list);
        } else if (val.getM() != null) {
            Map<String, AttributeValue> map = new HashMap<>(val.getM().size());
            for (Entry<String, AttributeValue> pair : val.getM().entrySet()) {
                if (!sourceDestinationMap.containsKey(pair.getValue())) {
                    sourceDestinationMap.put(pair.getValue(), clone(pair.getValue(), sourceDestinationMap));
                }
                map.put(pair.getKey(), sourceDestinationMap.get(pair.getValue()));
            }
            clonedVal.setM(map);
        }
        return clonedVal;
    }

    public static final int computeWcu(int bytes) {
        return Math.max(1, Integer.divideUnsigned(bytes, ONE_KILOBYTE));
    }

    private static final Charset UTF8 = Charset.forName("UTF8");
    // Each List element has 1 byte overhead for type. Adding 1 byte to account for it in item size
    public static final int BASE_LOGICAL_SIZE_OF_NESTED_TYPES = 1;
    protected static int LOGICAL_SIZE_OF_EMPTY_DOCUMENT = 3;
    public static final int MAX_NUMBER_OF_BYTES_FOR_NUMBER = 21;

    /**Calculate attribute value size*/
    private static int calculateAttributeSizeInBytes(AttributeValue value) {
        int attrValSize = 0;
        if(value == null) {
            return attrValSize;
        }

        if (value.getB() != null) {
            ByteBuffer b = value.getB();
            attrValSize += b.remaining();
        } else if (value.getS() != null) {
            String s = value.getS();
            attrValSize += s.getBytes(UTF8).length;
        } else if (value.getN() != null) {
            attrValSize += MAX_NUMBER_OF_BYTES_FOR_NUMBER;
        } else if (value.getBS() != null) {
            List<ByteBuffer> bs = value.getBS();
            for (ByteBuffer b : bs) {
                if ( b!= null) {
                    attrValSize += b.remaining();
                }
            }
        } else if (value.getSS() != null) {
            List<String> ss = value.getSS();
            for (String s : ss) {
                if (s != null) {
                    attrValSize += s.getBytes(UTF8).length;
                }
            }
        } else if (value.getNS() != null) {
            List<String> ns = value.getNS();
            for (String n : ns) {
                if (n != null) {
                    attrValSize += MAX_NUMBER_OF_BYTES_FOR_NUMBER;
                }
            }
        } else if (value.getBOOL() != null) {
            attrValSize += 1;
        } else if (value.getNULL() != null) {
            attrValSize += 1;
        } else if (value.getM() != null) {
            for(Map.Entry<String, AttributeValue> entry : value.getM().entrySet()) {
                attrValSize += entry.getKey().getBytes(UTF8).length;
                attrValSize += calculateAttributeSizeInBytes(entry.getValue());
                attrValSize += BASE_LOGICAL_SIZE_OF_NESTED_TYPES;
            }
            attrValSize += LOGICAL_SIZE_OF_EMPTY_DOCUMENT;
        } else if (value.getL() != null) {
            List<AttributeValue> list = value.getL();
            for (Integer i= 0; i < list.size(); i++) {
                attrValSize += calculateAttributeSizeInBytes(list.get(i));
                attrValSize += BASE_LOGICAL_SIZE_OF_NESTED_TYPES;
            }
            attrValSize += LOGICAL_SIZE_OF_EMPTY_DOCUMENT;
        }
        return attrValSize;
    }

    public static int calculateItemUpdateSizeInBytes(Map<String, AttributeValueUpdate> item) {
        int size = 0;

        if (item == null) {
            return size;
        }

        for (Map.Entry<String, AttributeValueUpdate> entry : item.entrySet()) {
            String name = entry.getKey();
            AttributeValueUpdate update = entry.getValue();
            size += name.getBytes(UTF8).length;
            size += calculateAttributeSizeInBytes(update.getValue());
        }
        return size;
    }

    public static int calculateItemSizeInBytes(Map<String, AttributeValue> item) {
        int size = 0;

        if (item == null) {
            return size;
        }

        for (Map.Entry<String, AttributeValue> entry : item.entrySet()) {
            String name = entry.getKey();
            AttributeValue value = entry.getValue();
            size += name.getBytes(UTF8).length;
            size += calculateAttributeSizeInBytes(value);
        }
        return size;
    }

    public long getMaxRetries() {
        return maxRetries;
    }

    public long getRetryMillis() {
        return retryMillis;
    }

    public boolean isEmbedded() {
        return embedded;
    }

    public String getListTablesApiName() {
        return listTablesApiName;
    }
}
