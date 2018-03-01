/*
 * Copyright 2014-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.locking.PermanentLockingException;
import org.janusgraph.util.stats.MetricManager;

import com.amazon.janusgraph.diskstorage.dynamodb.ExponentialBackoff.Scan;
import com.amazon.janusgraph.diskstorage.dynamodb.iterator.ParallelScanner;
import com.amazon.janusgraph.diskstorage.dynamodb.iterator.ScanSegmentWorker;
import com.amazon.janusgraph.diskstorage.dynamodb.mutation.MutateWorker;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
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
import com.amazonaws.util.AwsHostNameUtils;
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
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * A wrapper on top of the DynamoDB client API that self-throttles using metric-based and context-aware
 * estimates for all Read APIs and for DeleteItem, and that self-throttles using accurate upper-bound
 * estimates of item size for PutItem and UpdateItem. Has a thread pool and is able to do parallel
 * UpdateItem / DeleteItem, Query, and Scan calls.
 *
 * @author Alexander Patrikalakis
 *
 */
@Slf4j
public class DynamoDbDelegate  {
    private static final String PAGES = "Pages";
    private static final String CREATE_TABLE = "CreateTable";
    private static final String DELETE_TABLE = "DeleteTable";
    private static final String MUTATE_ITEM = "MutateItem";
    private static final String HASH_RANGE_KEY_SIZE_LIMIT = "Hash primary key values must be under 2048 bytes, and range primary key values must be under 1024 bytes";
    private static final String UPDATE_ITEM_SIZE_LIMIT = "Item size to update has exceeded the maximum allowed size";
    private static final String USER_AGENT = "x-amz-user-agent";
    private static final String PUT_ITEM = "PutItem";
    private static final String BATCH_WRITE_ITEM = "BatchWriteItem";
    private static final String DESCRIBE_TABLE = "DescribeTable";
    static final String UPDATE_ITEM = "UpdateItem";
    static final String DELETE_ITEM = "DeleteItem";
    static final String QUERY = "Query";
    static final String GET_ITEM = "GetItem";
    public static final String SCAN = "Scan";

    private static final Charset UTF8 = Charset.forName("UTF8");
    // Each List element has 1 byte overhead for type. Adding 1 byte to account for it in item size
    private static final int BASE_LOGICAL_SIZE_OF_NESTED_TYPES = 1;
    private static final int LOGICAL_SIZE_OF_EMPTY_DOCUMENT = 3;
    private static final int MAX_NUMBER_OF_BYTES_FOR_NUMBER = 21;

    private static final int ONE_KILOBYTE = 1024;
    private static final long CONTROL_PLANE_RETRY_DELAY_MS = 1000;
    private static final String LIST_TABLES = "ListTables";
    public static final int BATCH_WRITE_MAX_NUMBER_OF_ITEMS = 25;

    private final AmazonDynamoDB client;
    private final ThreadPoolExecutor clientThreadPool;
    private final Map<String, RateLimiter> readRateLimit;
    private final Map<String, RateLimiter> writeRateLimit;
    private final RateLimiter controlPlaneRateLimiter;
    private final int maxConcurrentUsers;
    @Getter
    private final long maxRetries;
    @Getter
    private final long retryMillis;
    @Getter
    private final boolean embedded = false;
    @Getter
    private final String listTablesApiName;
    private final String executorGaugeName;
    private final String metricsPrefix;

    public DynamoDbDelegate(final String endpoint, final String region, final AWSCredentialsProvider provider,
        final ClientConfiguration clientConfig, final Configuration titanConfig,
        final Map<String, RateLimiter> readRateLimit, final Map<String, RateLimiter> writeRateLimit,
        final long maxRetries, final long retryMillis, final String prefix, final String metricsPrefix,
        final RateLimiter controlPlaneRateLimiter) {
        if (prefix == null) {
            throw new IllegalArgumentException("prefix must be set");
        }
        if (metricsPrefix == null || metricsPrefix.isEmpty()) {
            throw new IllegalArgumentException("metrics-prefix may not be null or empty");
        }
        this.metricsPrefix = metricsPrefix;
        executorGaugeName = String.format("%s.%s_executor-queue-size", this.metricsPrefix, prefix);
        clientThreadPool = getPoolFromNs(titanConfig);
        if (!MetricManager.INSTANCE.getRegistry().getNames().contains(executorGaugeName)) {
            MetricManager.INSTANCE.getRegistry().register(executorGaugeName, (Gauge<Integer>) () -> clientThreadPool.getQueue().size());
        }

        client = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(provider)
                .withClientConfiguration(clientConfig)
                .withEndpointConfiguration(getEndpointConfiguration(Optional.ofNullable(endpoint), region))
            .build();
        this.readRateLimit = readRateLimit;
        this.writeRateLimit = writeRateLimit;
        this.controlPlaneRateLimiter = controlPlaneRateLimiter;
        this.maxConcurrentUsers = titanConfig.get(Constants.DYNAMODB_CLIENT_EXECUTOR_MAX_CONCURRENT_OPERATIONS);
        this.maxRetries = maxRetries;
        this.retryMillis = retryMillis;
        if (maxConcurrentUsers < 1) {
            throw new IllegalArgumentException("need at least one user otherwise wont make progress on scan");
        }
        this.listTablesApiName = String.format("%s_ListTables", prefix);
    }

    static ThreadPoolExecutor getPoolFromNs(final Configuration ns) {
        final int maxQueueSize = ns.get(Constants.DYNAMODB_CLIENT_EXECUTOR_QUEUE_MAX_LENGTH);
        final ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("getDelegate-%d").build();
        //begin adaptation of constructor at
        //https://github.com/buka/titan/blob/master/src/main/java/com/thinkaurelius/titan/diskstorage/dynamodb/DynamoDBClient.java#L104
        final int maxPoolSize = ns.get(Constants.DYNAMODB_CLIENT_EXECUTOR_MAX_POOL_SIZE);
        final int corePoolSize = ns.get(Constants.DYNAMODB_CLIENT_EXECUTOR_CORE_POOL_SIZE);
        final long keepAlive = ns.get(Constants.DYNAMODB_CLIENT_EXECUTOR_KEEP_ALIVE);
        final ThreadPoolExecutor executor = new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAlive,
            TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(maxQueueSize), factory, new ThreadPoolExecutor.CallerRunsPolicy());
        //end adaptation of constructor at
        //https://github.com/buka/titan/blob/master/src/main/java/com/thinkaurelius/titan/diskstorage/dynamodb/DynamoDBClient.java#L104
        executor.allowCoreThreadTimeOut(false);
        executor.prestartAllCoreThreads();
        return executor;
    }

    @VisibleForTesting
    static AwsClientBuilder.EndpointConfiguration getEndpointConfiguration(final Optional<String> endpoint, final String signingRegion) {
        Preconditions.checkArgument(endpoint != null, "must provide an optional endpoint and not null");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(signingRegion), "must provide a signing region");
        final String expectedServiceEndpoint = "https://" + Region.getRegion(Regions.fromName(signingRegion)).getServiceEndpoint(AmazonDynamoDB.ENDPOINT_PREFIX);
        if (endpoint.isPresent() && !Strings.isNullOrEmpty(endpoint.get())) {
            final String regionParsedFromEndpoint = AwsHostNameUtils.parseRegion(endpoint.get(), AmazonDynamoDB.ENDPOINT_PREFIX);
            Preconditions.checkArgument(regionParsedFromEndpoint == null || signingRegion.equals(regionParsedFromEndpoint));
            return new AwsClientBuilder.EndpointConfiguration(endpoint.get(), signingRegion);
        } else {
            //Regions.fromName will throw IllegalArgumentException if signingRegion is not valid.
            return new AwsClientBuilder.EndpointConfiguration(expectedServiceEndpoint, signingRegion);
        }
    }

    private <T extends AmazonWebServiceRequest> T setUserAgent(final T request) {
        request.putCustomRequestHeader(USER_AGENT, Constants.JANUSGRAPH_USER_AGENT);
        return request;
    }

    private BackendException processDynamoDbApiException(final Throwable e, final String apiName, final String tableName) {
        Preconditions.checkArgument(apiName != null);
        Preconditions.checkArgument(!apiName.isEmpty());
        final String prefix;
        if (tableName == null) {
            prefix = apiName;
        } else {
            prefix = String.format("%s_%s", apiName, tableName);
        }
        final String message = String.format("%s %s", prefix, e.getMessage());
        if (e instanceof ResourceNotFoundException) {
            return new BackendNotFoundException(String.format("%s; table not found", message), e);
        } else if (e instanceof ConditionalCheckFailedException) {
            return new PermanentLockingException(message, e);
        } else if (e instanceof AmazonServiceException) {
            if (e.getMessage() != null
                && (e.getMessage().contains(HASH_RANGE_KEY_SIZE_LIMIT) || e.getMessage().contains(UPDATE_ITEM_SIZE_LIMIT))) {
                return new PermanentBackendException(message, e);
            } else {
                return new TemporaryBackendException(message, e);
            }
        } else if (e instanceof AmazonClientException) { //all client exceptions are retriable by default
            return new TemporaryBackendException(message, e);
        } else if (e instanceof SocketException) { //sometimes this doesn't get caught by SDK
            return new TemporaryBackendException(message, e);
        }
        // unknown exception type
        return new PermanentBackendException(message, e);
    }

    public ScanResult scan(final ScanRequest request, final int permitsToConsume) throws BackendException {
        setUserAgent(request);
        ScanResult result;
        timedReadThrottle(SCAN, request.getTableName(), permitsToConsume);

        final Timer.Context apiTimerContext = getTimerContext(SCAN, request.getTableName());
        try {
            result = client.scan(request);
        } catch (Exception e) {
            throw processDynamoDbApiException(e, SCAN, request.getTableName());
        } finally {
            apiTimerContext.stop();
        }
        meterConsumedCapacity(SCAN, result.getConsumedCapacity());
        measureItemCount(SCAN, request.getTableName(), result.getCount());
        return result;
    }

    ParallelScanner getParallelScanCompletionService(final ScanRequest initialRequest) throws BackendException {
        final int segments = Math.max(1, clientThreadPool.getMaximumPoolSize() / maxConcurrentUsers);
        final ParallelScanner completion = new ParallelScanner(clientThreadPool, segments, this);

        for (int segment = 0; segment < segments; segment++) {
            // dont need to set user agent here because ExponentialBackoff.Scan
            // calls DynamoDbDelegate.scan which sets it
            final ScanRequest scanSegment = copyScanRequest(initialRequest).withTotalSegments(segments).withSegment(segment);
            completion.addWorker(new ScanSegmentWorker(this, scanSegment), segment);
        }

        return completion;
    }


    public Future<ScanResult> scanAsync(final ScanRequest request, final int permitsToConsume) {
        return clientThreadPool.submit(() -> {
            final Scan backoff = new Scan(request, this, permitsToConsume);
            return backoff.runWithBackoff();
        });
    }

    public static ScanRequest copyScanRequest(final ScanRequest request) {
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

    void parallelMutate(final List<MutateWorker> workers) throws BackendException {
        final CompletionService<Void> completion = new ExecutorCompletionService<>(clientThreadPool);
        final List<Future<Void>> futures = Lists.newLinkedList();
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
                    // fail out because janusgraph does not poll this thread for interrupted anywhere
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

    List<QueryResultWrapper> parallelQuery(final List<QueryWorker> queryWorkers) throws BackendException {
        final CompletionService<QueryResultWrapper> completionService = new ExecutorCompletionService<>(clientThreadPool);

        final List<Future<QueryResultWrapper>> futures = Lists.newLinkedList();
        for (QueryWorker worker : queryWorkers) {
            futures.add(completionService.submit(worker));
        }

        boolean interrupted = false;
        final List<QueryResultWrapper> results = Lists.newLinkedList();
        try {
            for (int i = 0; i < queryWorkers.size(); i++) {
                try {
                    final QueryResultWrapper result = completionService.take().get();
                    results.add(result);
                } catch (InterruptedException e) {
                    interrupted = true;
                    // fail out because janusgraph does not poll this thread for interrupted anywhere
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

    Map<StaticBuffer, GetItemResult> parallelGetItem(final List<GetItemWorker> workers) throws BackendException {
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
                    final GetItemResultWrapper result = completionService.take().get();
                    results.put(result.getJanusGraphKey(), result.getDynamoDBResult());
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

    public BackendException unwrapExecutionException(final ExecutionException e, final String apiName) {
        final Throwable cause = e.getCause();
        if (cause instanceof BackendException) {
            return (BackendException) cause; //already translated
        } else {
             //ok not to drill down to specific because would have thrown permanentbackend exception for other
            return processDynamoDbApiException(cause, apiName, null /*tableName*/);
        }
    }

    GetItemResult getItem(final GetItemRequest request) throws BackendException {
        setUserAgent(request);
        GetItemResult result;
        timedReadThrottle(GET_ITEM, request.getTableName(), estimateCapacityUnits(GET_ITEM, request.getTableName()));
        final Timer.Context apiTimerContext = getTimerContext(GET_ITEM, request.getTableName());
        try {
            result = client.getItem(request);
        } catch (Exception e) {
            throw processDynamoDbApiException(e, GET_ITEM, request.getTableName());
        } finally {
            apiTimerContext.stop();
        }
        meterConsumedCapacity(GET_ITEM, result.getConsumedCapacity());
        return result;
    }

    public BatchWriteItemResult batchWriteItem(final BatchWriteItemRequest batchRequest) throws BackendException {
        int count = 0;
        for (Entry<String, List<WriteRequest>> entry : batchRequest.getRequestItems().entrySet()) {
            final String tableName = entry.getKey();
            final List<WriteRequest> requests = entry.getValue();
            count += requests.size();
            if (count > BATCH_WRITE_MAX_NUMBER_OF_ITEMS) {
                throw new IllegalArgumentException("cant have more than 25 requests in a batchwrite");
            }
            for (final WriteRequest request : requests) {
                if ((request.getPutRequest() != null) == (request.getDeleteRequest() != null)) {
                    throw new IllegalArgumentException("Exactly one of PutRequest or DeleteRequest must be set in each WriteRequest in a batch write operation");
                }
                final int wcu;
                final String apiName;
                if (request.getPutRequest() != null) {
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
            throw processDynamoDbApiException(e, BATCH_WRITE_ITEM, null /*tableName*/);
        } finally {
            apiTimerContext.stop();
        }
        if (result.getConsumedCapacity() != null) {
            for (ConsumedCapacity ccu : result.getConsumedCapacity()) {
                meterConsumedCapacity(BATCH_WRITE_ITEM, ccu);
            }
        }
        return result;
    }

    public QueryResult query(final QueryRequest request, final int permitsToConsume) throws BackendException {
        setUserAgent(request);
        QueryResult result;
        timedReadThrottle(QUERY, request.getTableName(), permitsToConsume);
        final Timer.Context apiTimerContext = getTimerContext(QUERY, request.getTableName());
        try {
            result = client.query(request);
        } catch (Exception e) {
            throw processDynamoDbApiException(e, QUERY, request.getTableName());
        } finally {
            apiTimerContext.stop();
        }
        meterConsumedCapacity(QUERY, result.getConsumedCapacity());
        measureItemCount(QUERY, request.getTableName(), result.getCount());
        return result;
    }

    public PutItemResult putItem(final PutItemRequest request) throws BackendException {
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
            throw processDynamoDbApiException(e, PUT_ITEM, request.getTableName());
        } finally {
            apiTimerContext.stop();
        }
        meterConsumedCapacity(PUT_ITEM, result.getConsumedCapacity());

        return result;
    }

    UpdateItemResult updateItem(final UpdateItemRequest request) throws BackendException {
        setUserAgent(request);
        UpdateItemResult result;
        final int bytes;
        if (request.getUpdateExpression() != null) {
            bytes = calculateExpressionBasedUpdateSize(request);
        } else {
            bytes = calculateItemUpdateSizeInBytes(request.getAttributeUpdates());
        }
        getBytesHistogram(UPDATE_ITEM, request.getTableName()).update(bytes);
        final int wcu = computeWcu(bytes);
        timedWriteThrottle(UPDATE_ITEM, request.getTableName(), wcu);

        final Timer.Context apiTimerContext = getTimerContext(UPDATE_ITEM, request.getTableName());
        try {
            result = client.updateItem(request);
        } catch (Exception e) {
            throw processDynamoDbApiException(e, UPDATE_ITEM, request.getTableName());
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
    private int calculateExpressionBasedUpdateSize(final UpdateItemRequest request) {
        if (request == null || request.getUpdateExpression() == null) {
            throw new IllegalArgumentException("request did not use update expression");
        }
        int size = calculateItemSizeInBytes(request.getKey());
        for (AttributeValue value : request.getExpressionAttributeValues().values()) {
            size += calculateAttributeSizeInBytes(value);
        }
        return size;
    }

    DeleteItemResult deleteItem(final DeleteItemRequest request) throws BackendException {
        setUserAgent(request);
        DeleteItemResult result;
        final int wcu = estimateCapacityUnits(DELETE_ITEM, request.getTableName());
        timedWriteThrottle(DELETE_ITEM, request.getTableName(), wcu);

        final Timer.Context apiTimerContext = getTimerContext(DELETE_ITEM, request.getTableName());
        try {
            result = client.deleteItem(request);
        } catch (Exception e) {
            throw processDynamoDbApiException(e, DELETE_ITEM, request.getTableName());
        } finally {
            apiTimerContext.stop();
        }
        meterConsumedCapacity(DELETE_ITEM, result.getConsumedCapacity());

        return result;
    }

    public int estimateCapacityUnits(final String apiName, final String tableName) {
        int cu = 1;
        final Meter apiCcuMeter = getConsumedCapacityMeter(apiName, tableName);
        final Timer apiTimer = getTimer(apiName, tableName);

        if (apiCcuMeter != null && apiTimer != null && apiTimer.getCount() > 0) {
            cu = (int) Math.round(Math.max(1.0, (double) apiCcuMeter.getCount() / (double) apiTimer.getCount()));
        }
        return cu;
    }

    private RateLimiter readRateLimit(final String tableName) {
        return readRateLimit.get(tableName);
    }

    private RateLimiter writeRateLimit(final String tableName) {
        return writeRateLimit.get(tableName);
    }

    private void timedWriteThrottle(final String apiName, final String tableName, final int permits) {
        timedThrottle(apiName, writeRateLimit(tableName), tableName, permits);
    }

    private void timedReadThrottle(final String apiName, final String tableName, final int permits) {
        timedThrottle(apiName, readRateLimit(tableName), tableName, permits);
    }

    private void timedThrottle(final String apiName, final RateLimiter limiter, final String tableName, final int permits) {
        if (limiter == null) {
            throw new IllegalArgumentException("limiter for " + apiName + " on table " + tableName + " was null");
        }
        final Timer.Context throttleTimerCtxt = getTimerContext(String.format("%sThrottling", apiName), tableName);
        try {
            limiter.acquire(permits);
        } finally {
            throttleTimerCtxt.stop();
        }
    }

    ListTablesResult listTables(final ListTablesRequest request) throws BackendException {
        controlPlaneRateLimiter.acquire();
        final Timer.Context apiTimerContext = getTimerContext(listTablesApiName, null /*tableName*/);
        ListTablesResult result;
        try {
            result = client.listTables(request);
        } catch (final Exception e) {
            throw processDynamoDbApiException(e, LIST_TABLES, null /*tableName*/);
        } finally {
            apiTimerContext.stop();
        }
        return result;
    }

    public ListTablesResult listAllTables() throws BackendException {
        final ListTablesWorker worker = new ListTablesWorker(this);
        worker.call();
        return worker.getMergedPages();
    }

    private TableDescription describeTable(final String tableName) throws BackendException {
        return describeTable(new DescribeTableRequest().withTableName(tableName)).getTable();
    }

    private DescribeTableResult describeTable(final DescribeTableRequest request) throws BackendException {
        controlPlaneRateLimiter.acquire();
        final Timer.Context apiTimerContext = getTimerContext(DESCRIBE_TABLE, request.getTableName());
        DescribeTableResult result;
        try {
            result = client.describeTable(request);
        } catch (final Exception e) {
            throw processDynamoDbApiException(e, DESCRIBE_TABLE, request.getTableName());
        } finally {
            apiTimerContext.stop();
        }
        return result;
    }

    public DeleteTableResult deleteTable(final DeleteTableRequest request) throws BackendException {
        controlPlaneRateLimiter.acquire();
        final Timer.Context apiTimerContext = getTimerContext(DELETE_TABLE, request.getTableName());
        DeleteTableResult result;
        try {
            result = client.deleteTable(request);
        } catch (Exception e) {
            throw processDynamoDbApiException(e, DELETE_TABLE, request.getTableName());
        } finally {
            apiTimerContext.stop();
        }
        return result;
    }

    private void interruptibleSleep(final long millis) {
        boolean interrupted = false;
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            interrupted = true;
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    boolean ensureTableDeleted(final String tableName) throws BackendException {
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
        } while (!successFlag && retryCount < maxRetries);
        if (!successFlag) {
            throw new PermanentBackendException("Table deletion not completed after retrying " + maxRetries + " times");
        }
        return successFlag;
    }

    DeleteTableResult deleteTable(final String tableName) throws BackendException {
        return deleteTable(new DeleteTableRequest().withTableName(tableName));
    }

    private CreateTableResult createTable(final CreateTableRequest request) throws BackendException {
        controlPlaneRateLimiter.acquire();
        final Timer.Context apiTimerContext = getTimerContext(CREATE_TABLE, request.getTableName());
        CreateTableResult result;
        try {
            result = client.createTable(request);
        } catch (final Exception e) {
            throw processDynamoDbApiException(e, CREATE_TABLE, request.getTableName());
        } finally {
            apiTimerContext.stop();
        }
        return result;
    }

    private static boolean isTableAcceptingWrites(final String status) {
        return isTableStatus(TableStatus.ACTIVE, status) || isTableStatus(TableStatus.UPDATING, status);
    }

    private static boolean isTableStatus(final TableStatus constant, final String status) {
        return constant.toString().equals(status);
    }

    public void waitForTableCreation(final String tableName, final boolean verifyIndexesList,
        final List<LocalSecondaryIndexDescription> expectedLsiList, final List<GlobalSecondaryIndexDescription> expectedGsiList) throws BackendException {
        boolean successFlag = false;
        int retryCount = 0;
        while (!successFlag && retryCount < maxRetries) {
            try {
                boolean areAllGsisActive = true;
                final TableDescription td = describeTable(tableName);
                if (verifyIndexesList) {
                    final Set<LocalSecondaryIndexDescription> expectedLSIs = new HashSet<LocalSecondaryIndexDescription>();
                    if (expectedLsiList != null) {
                        expectedLSIs.addAll(expectedLsiList);
                    }
                    final Set<LocalSecondaryIndexDescription> actualLSIs = new HashSet<LocalSecondaryIndexDescription>();
                    if (td.getLocalSecondaryIndexes() != null) {
                        actualLSIs.addAll(td.getLocalSecondaryIndexes());
                    }
                    // the lsi list should be there even if the table is in creating state
                    if (!(expectedLsiList == null && td.getLocalSecondaryIndexes() == null || expectedLSIs.equals(actualLSIs))) {
                        throw new PermanentBackendException("LSI list is not as expected during table creation. expectedLsiList="
                            + expectedLsiList.toString() + "; table description=" + td.toString());
                    }

                    // ignore the status of all GSIs since they will mess up .equals()
                    if (td.getGlobalSecondaryIndexes() != null) {
                        for (final GlobalSecondaryIndexDescription gDesc : td.getGlobalSecondaryIndexes()) {
                            if (!isTableAcceptingWrites(gDesc.getIndexStatus())) {
                                areAllGsisActive = false;
                                break;
                            }
                        }
                    }

                    // the gsi list should be there even if the table is in creating state
                    if (!areGsisSameConfiguration(expectedGsiList, td.getGlobalSecondaryIndexes())) {
                        throw new PermanentBackendException("GSI list is not as expected during table creation. expectedGsiList="
                            + expectedGsiList.toString() + "; table description=" + td.toString());
                    }
                }
                successFlag = isTableAcceptingWrites(td.getTableStatus()) && areAllGsisActive;
            } catch (BackendNotFoundException ignore) {
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

    private static boolean areGsisSameConfiguration(final List<GlobalSecondaryIndexDescription> g1,
            final List<GlobalSecondaryIndexDescription> g2) {
        if (g1 == null) {
            return g2 == null;
        }
        if (g1.size() != g2.size()) {
            return false;
        }
        // make copy of the lists because we don't want to mutate the lists
        final ArrayList<GlobalSecondaryIndexDescription> g1clone = new ArrayList<>(g1.size());
        g1clone.addAll(g1);
        final ArrayList<GlobalSecondaryIndexDescription> g2clone = new ArrayList<>(g2.size());
        g1clone.addAll(g2);

        for (final GlobalSecondaryIndexDescription gi1 : g1) {
            for (final GlobalSecondaryIndexDescription  gi2 : g2) {
                if (areGsisSameConfiguration(gi1, gi2)) {
                    g1clone.remove(gi1);
                    g2clone.remove(gi2);
                    break;
                }
            }
        }
        return g1clone.isEmpty() || g2clone.isEmpty();
    }

    private static boolean areGsisSameConfiguration(final GlobalSecondaryIndexDescription g1, final GlobalSecondaryIndexDescription g2) {
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
            new HashSet<>(Optional.ofNullable(g1.getProjection().getNonKeyAttributes()).orElse(Collections.emptyList()));
        final Set<String> projectionNonKeyAttributesG2 =
            new HashSet<>(Optional.ofNullable(g2.getProjection().getNonKeyAttributes()).orElse(Collections.emptyList()));
        builder.append(projectionNonKeyAttributesG1, projectionNonKeyAttributesG2);

        return builder.build();
    }

    void createTableAndWaitForActive(final CreateTableRequest request) throws BackendException {
        final String tableName = request.getTableName();
        Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName), "Table name was null or empty");
        final TableDescription desc;
        try {
            desc = this.describeTable(tableName);
            if (null != desc && isTableAcceptingWrites(desc.getTableStatus())) {
                return; //store existed
            }
        } catch (BackendNotFoundException e) {
            log.debug(tableName + " did not exist yet, creating it", e);
        }

        createTable(request);
        waitForTableCreation(tableName, false /*verifyIndexesList*/, null /*expectedLsiList*/, null /*expectedGsiList*/);
    }

    public void shutdown() {
        MetricManager.INSTANCE.getRegistry().remove(executorGaugeName);
        // TODO(amcp) figure out a way to make the thread pool not be static
        // https://github.com/awslabs/dynamodb-titan-storage-backend/issues/48
        client.shutdown();
    }

    private Timer getTimer(final String apiName, final String tableName) {
        return MetricManager.INSTANCE.getTimer(getMeterName(apiName, tableName));
    }
    final Timer.Context getTimerContext(final String apiName, final String tableName) {
        return getTimer(apiName, tableName).time();
    }
    final Meter getMeter(final String meterName) {
        return MetricManager.INSTANCE.getRegistry().meter(meterName);
    }
    private String getItemCountMeterName(final String apiName, final String tableName) {
        return getMeterName(String.format("%sItemCount", apiName), tableName);
    }
    private void measureItemCount(final String apiName, final String tableName, final long itemCount) {
        getMeter(getItemCountMeterName(apiName, tableName)).mark(itemCount);
        getCounter(apiName, tableName, "ItemCountCounter").inc(itemCount);
    }
    private Counter getCounter(final String apiName, final String tableName, final String quantity) {
        return MetricManager.INSTANCE.getCounter(getQuantityName(apiName, tableName, quantity));
    }
    private void meterConsumedCapacity(final String apiName, final ConsumedCapacity ccu) {
        if (ccu != null) {
            getConsumedCapacityMeter(apiName, ccu.getTableName()).mark(Math.round(ccu.getCapacityUnits()));
        }
    }
    private String getQuantityName(final String apiName, final String tableName, final String quantity) {
        return getMeterName(String.format("%s%s", apiName, quantity), tableName);
    }
    private Meter getQuantityMeter(final String apiName, final String tableName, final String quantity) {
        return getMeter(getQuantityName(apiName, tableName, quantity));
    }
    private Meter getConsumedCapacityMeter(final String apiName, final String tableName) {
        return getQuantityMeter(apiName, tableName, "ConsumedCapacity");
    }
    private Histogram getBytesHistogram(final String apiName, final String tableName) {
        return getHistogram(apiName, tableName, "Bytes");
    }
    private Histogram getHistogram(final String apiName, final String tableName, final String quantity) {
        return MetricManager.INSTANCE.getHistogram(getQuantityName(apiName, tableName, quantity));
    }
    public final Histogram getPagesHistogram(final String apiName, final String tableName) {
        return getHistogram(apiName, tableName, PAGES);
    }
    void updatePagesHistogram(final String apiName, final String tableName, final int pagesProcessed) {
        getHistogram(apiName, tableName, PAGES).update(pagesProcessed);
    }
    final String getMeterName(final String apiName, final String tableName) {
        if (tableName == null) {
            return String.format("%s.%s", metricsPrefix, apiName);
        }
        return String.format("%s.%s.%s", metricsPrefix, apiName, tableName);
    }
    final int getMaxConcurrentUsers() {
        return this.maxConcurrentUsers;
    }

    /**
     * Helper method that clones an item
     *
     * @param item the item to clone
     * @return a clone of item.
     */
    public static Map<String, AttributeValue> cloneItem(final Map<String, AttributeValue> item) {
        if (item == null) {
            return null;
        }
        final Map<String, AttributeValue> clonedItem = Maps.newHashMap();
        final IdentityHashMap<AttributeValue, AttributeValue> sourceDestinationMap = new IdentityHashMap<>();

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
    public static AttributeValue clone(final AttributeValue val, final IdentityHashMap<AttributeValue, AttributeValue> sourceDestinationMap) {
        if (val == null) {
            return null;
        }

        if (sourceDestinationMap.containsKey(val)) {
            return sourceDestinationMap.get(val);
        }

        final AttributeValue clonedVal = new AttributeValue();
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
            final List<AttributeValue> list = new ArrayList<>(val.getL().size());
            for (AttributeValue listItemValue : val.getL()) {
                if (!sourceDestinationMap.containsKey(listItemValue)) {
                    sourceDestinationMap.put(listItemValue, clone(listItemValue, sourceDestinationMap));
                }
                list.add(sourceDestinationMap.get(listItemValue));
            }
            clonedVal.setL(list);
        } else if (val.getM() != null) {
            final Map<String, AttributeValue> map = new HashMap<>(val.getM().size());
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

    public static final int computeWcu(final int bytes) {
        return Math.max(1, Integer.divideUnsigned(bytes, ONE_KILOBYTE));
    }

    /**Calculate attribute value size*/
    private static int calculateAttributeSizeInBytes(final AttributeValue value) {
        int attrValSize = 0;
        if (value == null) {
            return attrValSize;
        }

        if (value.getB() != null) {
            final ByteBuffer b = value.getB();
            attrValSize += b.remaining();
        } else if (value.getS() != null) {
            final String s = value.getS();
            attrValSize += s.getBytes(UTF8).length;
        } else if (value.getN() != null) {
            attrValSize += MAX_NUMBER_OF_BYTES_FOR_NUMBER;
        } else if (value.getBS() != null) {
            final List<ByteBuffer> bs = value.getBS();
            for (ByteBuffer b : bs) {
                if (b != null) {
                    attrValSize += b.remaining();
                }
            }
        } else if (value.getSS() != null) {
            final List<String> ss = value.getSS();
            for (String s : ss) {
                if (s != null) {
                    attrValSize += s.getBytes(UTF8).length;
                }
            }
        } else if (value.getNS() != null) {
            final List<String> ns = value.getNS();
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
            for (Map.Entry<String, AttributeValue> entry : value.getM().entrySet()) {
                attrValSize += entry.getKey().getBytes(UTF8).length;
                attrValSize += calculateAttributeSizeInBytes(entry.getValue());
                attrValSize += BASE_LOGICAL_SIZE_OF_NESTED_TYPES;
            }
            attrValSize += LOGICAL_SIZE_OF_EMPTY_DOCUMENT;
        } else if (value.getL() != null) {
            final List<AttributeValue> list = value.getL();
            for (Integer i = 0; i < list.size(); i++) {
                attrValSize += calculateAttributeSizeInBytes(list.get(i));
                attrValSize += BASE_LOGICAL_SIZE_OF_NESTED_TYPES;
            }
            attrValSize += LOGICAL_SIZE_OF_EMPTY_DOCUMENT;
        }
        return attrValSize;
    }

    public static int calculateItemUpdateSizeInBytes(final Map<String, AttributeValueUpdate> item) {
        int size = 0;

        if (item == null) {
            return size;
        }

        for (Map.Entry<String, AttributeValueUpdate> entry : item.entrySet()) {
            final String name = entry.getKey();
            final AttributeValueUpdate update = entry.getValue();
            size += name.getBytes(UTF8).length;
            size += calculateAttributeSizeInBytes(update.getValue());
        }
        return size;
    }

    public static int calculateItemSizeInBytes(final Map<String, AttributeValue> item) {
        int size = 0;

        if (item == null) {
            return size;
        }

        for (Map.Entry<String, AttributeValue> entry : item.entrySet()) {
            final String name = entry.getKey();
            final AttributeValue value = entry.getValue();
            size += name.getBytes(UTF8).length;
            size += calculateAttributeSizeInBytes(value);
        }
        return size;
    }
}
