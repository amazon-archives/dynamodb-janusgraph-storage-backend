/*
 * Copyright 2014-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Portions copyright Titan: Distributed Graph Database - Copyright 2012 and onwards Aurelius.
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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.RateLimiterCreator;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.util.stats.MetricManager;

/**
 * Operations setting up the DynamoDB client.
 *
 * @author Matthew Sowders
 * @author Alexander Patrikalakis
 *
 */
public class Client {
    private static final String VALIDATE_CREDENTIALS_CLASS_NAME = "Must provide either an AWSCredentials or AWSCredentialsProvider fully qualified class name";
    public static final double DEFAULT_BURST_BUCKET_SIZE_IN_SECONDS = 300.0;

    protected final MetricManager metrics = MetricManager.INSTANCE;

    private final Map<String, Long> capacityRead = new HashMap<>();
    private final Map<String, Long> capacityWrite = new HashMap<>();
    private final Map<String, BackendDataModel> dataModel = new HashMap<>();
    private final boolean forceConsistentRead;
    private final boolean enableParallelScan;
    private final Map<String, Integer> scanLimit = new HashMap<>();
    private final DynamoDBDelegate delegate;
    @VisibleForTesting
    final String endpoint;
    final String signingRegion;

    private final String prefix;

    public Client(com.thinkaurelius.titan.diskstorage.configuration.Configuration config) {
        String credentialsClassName = config.get(Constants.DYNAMODB_CREDENTIALS_CLASS_NAME);
        Class<?> clazz;
        try {
            clazz = Class.forName(credentialsClassName);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(VALIDATE_CREDENTIALS_CLASS_NAME, e);
        }

        String[] credentialsConstructorArgsValues = config.get(Constants.DYNAMODB_CREDENTIALS_CONSTRUCTOR_ARGS);
        final List<String> filteredArgList = new ArrayList<String>();
        for(Object obj : credentialsConstructorArgsValues) {
            final String str = obj.toString();
            if(!str.isEmpty()) {
                filteredArgList.add(str);
            }
        }

        AWSCredentialsProvider credentialsProvider;
        if (AWSCredentials.class.isAssignableFrom(clazz)) {
            AWSCredentials credentials = createCredentials(clazz, filteredArgList.toArray(new String[filteredArgList.size()]));
            credentialsProvider = new StaticCredentialsProvider(credentials);
        } else if (AWSCredentialsProvider.class.isAssignableFrom(clazz)) {
            credentialsProvider = createCredentialsProvider(clazz, credentialsConstructorArgsValues);
        } else {
            throw new IllegalArgumentException(VALIDATE_CREDENTIALS_CLASS_NAME);
        }
//begin adaptation of constructor at
//https://github.com/buka/titan/blob/master/src/main/java/com/thinkaurelius/titan/diskstorage/dynamodb/DynamoDBClient.java#L77
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.withConnectionTimeout(config.get(Constants.DYNAMODB_CLIENT_CONN_TIMEOUT)) //
                .withConnectionTTL(config.get(Constants.DYNAMODB_CLIENT_CONN_TTL)) //
                .withMaxConnections(config.get(Constants.DYNAMODB_CLIENT_MAX_CONN)) //
                .withMaxErrorRetry(config.get(Constants.DYNAMODB_CLIENT_MAX_ERROR_RETRY)) //
                .withGzip(config.get(Constants.DYNAMODB_CLIENT_USE_GZIP)) //
                .withReaper(config.get(Constants.DYNAMODB_CLIENT_USE_REAPER)) //
                .withUserAgentSuffix(config.get(Constants.DYNAMODB_CLIENT_USER_AGENT)) //
                .withSocketTimeout(config.get(Constants.DYNAMODB_CLIENT_SOCKET_TIMEOUT)) //
                .withSocketBufferSizeHints( //
                        config.get(Constants.DYNAMODB_CLIENT_SOCKET_BUFFER_SEND_HINT), //
                        config.get(Constants.DYNAMODB_CLIENT_SOCKET_BUFFER_RECV_HINT)) //
                .withProxyDomain(config.get(Constants.DYNAMODB_CLIENT_PROXY_DOMAIN)) //
                .withProxyWorkstation(config.get(Constants.DYNAMODB_CLIENT_PROXY_WORKSTATION)) //
                .withProxyHost(config.get(Constants.DYNAMODB_CLIENT_PROXY_HOST)) //
                .withProxyPort(config.get(Constants.DYNAMODB_CLIENT_PROXY_PORT)) //
                .withProxyUsername(config.get(Constants.DYNAMODB_CLIENT_PROXY_USERNAME)) //
                .withProxyPassword(config.get(Constants.DYNAMODB_CLIENT_PROXY_PASSWORD)); //

        forceConsistentRead = config.get(Constants.DYNAMODB_FORCE_CONSISTENT_READ);
//end adaptation of constructor at
//https://github.com/buka/titan/blob/master/src/main/java/com/thinkaurelius/titan/diskstorage/dynamodb/DynamoDBClient.java#L77
        enableParallelScan = config.get(Constants.DYNAMODB_ENABLE_PARALLEL_SCAN);
        prefix = config.get(Constants.DYNAMODB_TABLE_PREFIX);
        final String metricsPrefix = config.get(Constants.DYNAMODB_METRICS_PREFIX);

        final long maxRetries = config.get(Constants.DYNAMODB_MAX_SELF_THROTTLED_RETRIES);
        if(maxRetries < 0) {
            throw new IllegalArgumentException(Constants.DYNAMODB_MAX_SELF_THROTTLED_RETRIES.getName() + " must be at least 0");
        }
        final long retryMillis = config.get(Constants.DYNAMODB_INITIAL_RETRY_MILLIS);
        if(retryMillis <= 0) {
            throw new IllegalArgumentException(Constants.DYNAMODB_INITIAL_RETRY_MILLIS.getName() + " must be at least 1");
        }
        final double controlPlaneRate = config.get(Constants.DYNAMODB_CONTROL_PLANE_RATE);
        if(controlPlaneRate < 0) {
            throw new IllegalArgumentException("must have a positive control plane rate");
        }
        final RateLimiter controlPlaneRateLimiter = RateLimiter.create(controlPlaneRate);

        final Map<String, RateLimiter> readRateLimit = new HashMap<>();
        final Map<String, RateLimiter> writeRateLimit = new HashMap<>();

        Set<String> storeNames = new HashSet<String>(Constants.REQUIRED_BACKEND_STORES);
        storeNames.addAll(config.getContainedNamespaces(Constants.DYNAMODB_STORES_NAMESPACE));
        for(String storeName : storeNames) {
            setupStore(config, prefix, readRateLimit, writeRateLimit, storeName);
        }

        endpoint = config.get(Constants.DYNAMODB_CLIENT_ENDPOINT);
        signingRegion = TitanConfigUtil.getNullableConfigValue(config, Constants.DYNAMODB_CLIENT_SIGNING_REGION);
        delegate = new DynamoDBDelegate(endpoint, signingRegion, credentialsProvider,
            clientConfig, config, readRateLimit, writeRateLimit, maxRetries, retryMillis, prefix, metricsPrefix, controlPlaneRateLimiter);
    }

    public static final ThreadPoolExecutor getPoolFromNs(Configuration ns) {
        final int maxQueueSize = ns.get(Constants.DYNAMODB_CLIENT_EXECUTOR_QUEUE_MAX_LENGTH);
        final ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("delegate-%d").build();
//begin adaptation of constructor at
//https://github.com/buka/titan/blob/master/src/main/java/com/thinkaurelius/titan/diskstorage/dynamodb/DynamoDBClient.java#L104
        final int maxPoolSize = ns.get(Constants.DYNAMODB_CLIENT_EXECUTOR_MAX_POOL_SIZE);
        final int corePoolSize = ns.get(Constants.DYNAMODB_CLIENT_EXECUTOR_CORE_POOL_SIZE);
        final long keepAlive = ns.get(Constants.DYNAMODB_CLIENT_EXECUTOR_KEEP_ALIVE);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAlive,
            TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(maxQueueSize), factory, new ThreadPoolExecutor.CallerRunsPolicy());
//end adaptation of constructor at
//https://github.com/buka/titan/blob/master/src/main/java/com/thinkaurelius/titan/diskstorage/dynamodb/DynamoDBClient.java#L104
        executor.allowCoreThreadTimeOut(false);
        executor.prestartAllCoreThreads();
        return executor;
    }

    private void setupStore(com.thinkaurelius.titan.diskstorage.configuration.Configuration config, String prefix,
        final Map<String, RateLimiter> readRateLimit, final Map<String, RateLimiter> writeRateLimit, String store) {

        final String dataModel = config.get(Constants.STORES_DATA_MODEL, store);
        final int scanLimit = config.get(Constants.STORES_SCAN_LIMIT, store);
        final long readCapacity = config.get(Constants.STORES_CAPACITY_READ, store);
        final long writeCapacity = config.get(Constants.STORES_CAPACITY_WRITE, store);
        final double readRate = config.get(Constants.STORES_READ_RATE_LIMIT, store);
        final double writeRate = config.get(Constants.STORES_WRITE_RATE_LIMIT, store);

        String actualTableName = prefix + "_" + store;

        this.dataModel.put(store, BackendDataModel.valueOf(dataModel));
        this.capacityRead.put(actualTableName, readCapacity);
        this.capacityWrite.put(actualTableName, writeCapacity);
        readRateLimit.put(actualTableName, RateLimiterCreator.createBurstingLimiter(readRate, DEFAULT_BURST_BUCKET_SIZE_IN_SECONDS));
        writeRateLimit.put(actualTableName, RateLimiterCreator.createBurstingLimiter(writeRate, DEFAULT_BURST_BUCKET_SIZE_IN_SECONDS));
        this.scanLimit.put(actualTableName, scanLimit);
    }

    public DynamoDBDelegate delegate() {
        return delegate;
    }

    public boolean forceConsistentRead() {
        return forceConsistentRead;
    }

    public boolean enableParallelScan() {
        return enableParallelScan;
    }

    public long readCapacity(String tableName) {
        return capacityRead.get(tableName);
    }

    public long writeCapacity(String tableName) {
        return capacityWrite.get(tableName);
    }

    public BackendDataModel dataModel(String storeName) {
        return dataModel.get(storeName);
    }

    public int scanLimit(String tableName) {
        return scanLimit.get(tableName);
    }

    public static final AWSCredentialsProvider createAWSCredentialsProvider(String credentialsClassName,
        String[] credentialsConstructorArgsValues) {
        Class<?> clazz;
        try {
            clazz = Class.forName(credentialsClassName);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(VALIDATE_CREDENTIALS_CLASS_NAME, e);
        }
        AWSCredentialsProvider credentialsProvider;
        if (AWSCredentials.class.isAssignableFrom(clazz)) {
            AWSCredentials credentials = createCredentials(clazz, credentialsConstructorArgsValues);
            credentialsProvider = new StaticCredentialsProvider(credentials);
        } else if (AWSCredentialsProvider.class.isAssignableFrom(clazz)) {
            credentialsProvider = createCredentialsProvider(clazz, credentialsConstructorArgsValues);
        } else {
            throw new IllegalArgumentException(VALIDATE_CREDENTIALS_CLASS_NAME);
        }

        return credentialsProvider;
    }

    private static final AWSCredentialsProvider createCredentialsProvider(Class<?> clazz, String[] credentialsProviderConstructorArgs) {
        return (AWSCredentialsProvider) createInstance(clazz, credentialsProviderConstructorArgs);
    }

    private static final AWSCredentials createCredentials(Class<?> clazz, String[] credentialsConstructorArgs) {
        return (AWSCredentials) createInstance(clazz, credentialsConstructorArgs);
    }

    private static final Object createInstance(Class<?> clazz, String[] constructorArgs) {
        Class<?>[] constructorTypes;
        String[] actualArgs = constructorArgs;
        if (null == constructorArgs) {
            constructorTypes = new Class<?>[0];
        } else if (constructorArgs.length == 1 && Strings.isNullOrEmpty(constructorArgs[0])) {
            // Special case for empty constructors
            actualArgs = new String[0];
            constructorTypes = new Class<?>[0];
        } else {
            constructorTypes = new Class<?>[constructorArgs.length];
            for (int i = 0; i < constructorArgs.length; i++) {
                constructorTypes[i] = String.class;
            }
        }
        Constructor<?> constructor;
        try {
            constructor = clazz.getConstructor(constructorTypes);
        } catch (NoSuchMethodException | SecurityException e) {
            throw new IllegalArgumentException("Cannot access constructor:" + clazz.getCanonicalName() + "(" + constructorTypes.length + ")", e);
        }
        Object instance;
        try {
            instance = constructor.newInstance((Object[]) actualArgs);
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new IllegalArgumentException("Cannot create new instance:" + clazz.getCanonicalName(), e);
        }
        return instance;
    }

    public String getPrefix() {
        return prefix;
    }

}
