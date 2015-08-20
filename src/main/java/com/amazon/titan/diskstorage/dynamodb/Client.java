/*
 * Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.Configuration;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.thinkaurelius.titan.util.stats.MetricManager;

/**
 * Operations setting up the DynamoDB client.
 *
 * @author Matthew Sowders
 * @author Alexander Patrikalakis
 *
 */
public class Client {

    protected final MetricManager metrics = MetricManager.INSTANCE;

    private static final String VALIDATE_CREDENTIALS_CLASS_NAME = "Must provide either an AWSCredentials or AWSCredentialsProvider fully qualified class name to the config:"
            + Constants.CREDENTIALS_CLASS_NAME;
    final static String STORES_NS_PREFIX = "storage.dynamodb.stores";
    private final Map<String, Long> capacityRead = new HashMap<>();
    private final Map<String, Long> capacityWrite = new HashMap<>();
    private final Map<String, BackendDataModel> dataModel = new HashMap<>();
    private final boolean forceConsistentRead;
    private final boolean enableParallelScan;
    private final Map<String, Integer> scanLimit = new HashMap<>();
    private final DynamoDBDelegate delegate;
    @VisibleForTesting
    final String endpoint;

    private final String prefix;

    public Client(Configuration storageNamespace) {
        final Configuration dynamoDBNamespace = storageNamespace.subset(Constants.DYNAMODB_NS);
        final Configuration clientNamespace = dynamoDBNamespace.subset(Constants.CLIENT_NS);
        final Configuration proxyNamespace = clientNamespace.subset(Constants.CLIENT_PROXY_NS);
        final Configuration executorNamespace = clientNamespace.subset(Constants.CLIENT_EXECUTOR_NS);
        final Configuration credentialsNamespace = clientNamespace.subset(Constants.CLIENT_CREDENTIALS_NS);
        final Configuration socketNamespace = clientNamespace.subset(Constants.CLIENT_SOCKET_NS);

        final String credentialsClassName = credentialsNamespace.getString(Constants.CREDENTIALS_CLASS_NAME, Constants.CREDENTIALS_CLASS_NAME_DEFAULT);
        @SuppressWarnings("unchecked")
        final List<Object> credentialsConstructorArgsValueList =
            credentialsNamespace.containsKey(Constants.CREDENTIALS_CONSTRUCTOR_ARGS) ?
            (List<Object>) credentialsNamespace.getList(Constants.CREDENTIALS_CONSTRUCTOR_ARGS) :
            Constants.CREDENTIALS_CONSTRUCTOR_ARGS_DEFAULT;
        final List<String> filteredArgList = new ArrayList<String>();
        for(Object obj : credentialsConstructorArgsValueList) {
            final String str = obj.toString();
            if(!str.isEmpty()) {
                filteredArgList.add(str);
            }
        }
        final AWSCredentialsProvider credentialsProvider = createAWSCredentialsProvider(credentialsClassName, filteredArgList.toArray(new String[filteredArgList.size()]));

//begin adaptation of constructor at
//https://github.com/buka/titan/blob/master/src/main/java/com/thinkaurelius/titan/diskstorage/dynamodb/DynamoDBClient.java#L77
        final ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig
                .withConnectionTimeout(clientNamespace.getInt(Constants.CLIENT_CONN_TIMEOUT, Constants.CLIENT_CONN_TIMEOUT_DEFAULT))
                .withConnectionTTL(clientNamespace.getLong(Constants.CLIENT_CONN_TTL, Constants.CLIENT_CONN_TTL_DEFAULT))
                .withMaxConnections(clientNamespace.getInt(Constants.CLIENT_MAX_CONN, ClientConfiguration.DEFAULT_MAX_CONNECTIONS))
                .withMaxErrorRetry(clientNamespace.getInt(Constants.CLIENT_MAX_ERROR_RETRY, Constants.CLIENT_MAX_ERROR_RETRY_DEFAULT))
                .withGzip(clientNamespace.getBoolean(Constants.CLIENT_USE_GZIP, Constants.CLIENT_USE_GZIP_DEFAULT))
                .withReaper(clientNamespace.getBoolean(Constants.CLIENT_USE_REAPER, Constants.CLIENT_USE_REAPER_DEFAULT))
                .withUserAgent(clientNamespace.getString(Constants.CLIENT_USER_AGENT, ClientConfiguration.DEFAULT_USER_AGENT))
                .withSocketTimeout(socketNamespace.getInt(Constants.CLIENT_SOCKET_TIMEOUT, ClientConfiguration.DEFAULT_SOCKET_TIMEOUT))
                .withSocketBufferSizeHints(
                        socketNamespace.getInt(Constants.CLIENT_SOCKET_BUFFER_SEND_HINT, Constants.CLIENT_SOCKET_BUFFER_SEND_HINT_DEFAULT),
                        socketNamespace.getInt(Constants.CLIENT_SOCKET_BUFFER_RECV_HINT, Constants.CLIENT_SOCKET_BUFFER_RECV_HINT_DEFAULT))
                .withProxyDomain(proxyNamespace.getString(Constants.CLIENT_PROXY_DOMAIN))
                .withProxyWorkstation(proxyNamespace.getString(Constants.CLIENT_PROXY_WORKSTATION))
                .withProxyHost(proxyNamespace.getString(Constants.CLIENT_PROXY_HOST))
                .withProxyPort(proxyNamespace.getInt(Constants.CLIENT_PROXY_PORT, 0))
                .withProxyUsername(proxyNamespace.getString(Constants.CLIENT_PROXY_USERNAME))
                .withProxyPassword(proxyNamespace.getString(Constants.CLIENT_PROXY_PASSWORD));

        forceConsistentRead = dynamoDBNamespace.getBoolean(Constants.FORCE_CONSISTENT_READ, Constants.FORCE_CONSISTENT_READ_DEFAULT);
//end adaptation of constructor at
//https://github.com/buka/titan/blob/master/src/main/java/com/thinkaurelius/titan/diskstorage/dynamodb/DynamoDBClient.java#L77
        this.prefix = dynamoDBNamespace.getString(Constants.TABLE_PREFIX, Constants.TABLE_PREFIX_DEFAULT);
        enableParallelScan = dynamoDBNamespace.getBoolean(Constants.ENABLE_PARALLEL_SCAN, Constants.ENABLE_PARALLEL_SCAN_DEFAULT);
        final String metricsPrefix = dynamoDBNamespace.getString(Constants.METRICS_PREFIX, Constants.METRICS_PREFIX_DEFAULT);
        final long maxRetries = dynamoDBNamespace.getLong(Constants.TITAN_DYNAMODB_MAX_SELF_THROTTLED_RETRIES,
            Constants.TITAN_DYNAMODB_MAX_SELF_THROTTLED_RETRIES_DEFAULT);
        if(maxRetries < 0) {
            throw new IllegalArgumentException(Constants.TITAN_DYNAMODB_MAX_SELF_THROTTLED_RETRIES + " must be at least 0");
        }
        final long retryMillis = dynamoDBNamespace.getLong(Constants.TITAN_DYNAMODB_INTIAL_RETRY_MILLIS,
            Constants.TITAN_DYNAMODB_INTIAL_RETRY_MILLIS_DEFAULT);
        if(retryMillis <= 0) {
            throw new IllegalArgumentException(Constants.TITAN_DYNAMODB_INTIAL_RETRY_MILLIS + " must be at least 1");
        }
        final double controlPlaneRate = dynamoDBNamespace.getDouble(Constants.TITAN_DYNAMODB_CONTROL_PLANE_RATE, Constants.TITAN_DYNAMODB_CONTROL_PLANE_RATE_DEFAULT);
        if(controlPlaneRate < 0) {
            throw new IllegalArgumentException("must have a positive control plane rate");
        }
        final RateLimiter controlPlaneRateLimiter = RateLimiter.create(controlPlaneRate);

        final Map<String, RateLimiter> readRateLimit = new HashMap<>();
        final Map<String, RateLimiter> writeRateLimit = new HashMap<>();
        final Configuration stores = dynamoDBNamespace.subset(Constants.STORES_NS);
        final Set<String> storeNames = getContainedNamespaces(stores, STORES_NS_PREFIX);
        storeNames.addAll(Constants.BACKEND_STORE_NAMES); //to load defaults for stores if not configured
        for(String storeName : storeNames) {
            final Configuration store = stores.subset(storeName);
            setupStore(prefix, readRateLimit, writeRateLimit, store, storeName);
        }

        endpoint = clientNamespace.getString(Constants.CLIENT_ENDPOINT);
        delegate = new DynamoDBDelegate(endpoint, credentialsProvider,
            clientConfig, executorNamespace, readRateLimit, writeRateLimit, maxRetries, retryMillis, prefix, metricsPrefix, controlPlaneRateLimiter);
    }

    //begin titan code (I modified it):
    //https://github.com/thinkaurelius/titan/blob/47e9b35efaa4d1eb8722c1db0b1e599336a2c049/titan-core/src/main/java/com/thinkaurelius/titan/diskstorage/configuration/AbstractConfiguration.java#L39
    public static Set<String> getContainedNamespaces(final Configuration stores, final String prefix) {
        Set<String> result = Sets.newHashSet();

        @SuppressWarnings("unchecked")
        Iterator<String> storesIt = stores.getKeys();
        while(storesIt.hasNext()) {
            final String key = storesIt.next();
            if(!key.isEmpty()) {
                final String[] keyElements = key.split("\\.");
                final String ns = keyElements[0];
                result.add(ns);
            }
        }
        return result;
    }
    //end titan code

    public void setupStore(final String prefix, final Map<String, RateLimiter> readRateLimit,
        final Map<String, RateLimiter> writeRateLimit, final Configuration store, String storeName) {

        final int scanLimit = store.getInt(Constants.SCAN_LIMIT, Constants.SCAN_LIMIT_DEFAULT);
        final String dataModel = store.getString(Constants.DATA_MODEL, Constants.DATA_MODEL_DEFAULT);
        final long readCapacity = store.getLong(Constants.READ_CAPACITY, Constants.READ_CAPACITY_DEFAULT);
        final long writeCapacity = store.getLong(Constants.WRITE_CAPACITY, Constants.WRITE_CAPACITY_DEFAULT);
        final double readRate = store.getDouble(Constants.READ_RATE_LIMIT, Constants.READ_RATE_LIMIT_DEFAULT);
        final double writeRate = store.getDouble(Constants.WRITE_RATE_LIMIT, Constants.WRITE_RATE_LIMIT_DEFAULT);

        final String tableName = prefix + "_" + storeName;

        this.dataModel.put(storeName, BackendDataModel.valueOf(dataModel));
        this.capacityRead.put(tableName, readCapacity);
        this.capacityWrite.put(tableName, writeCapacity);
        readRateLimit.put(tableName, RateLimiter.create(readRate));
        writeRateLimit.put(tableName, RateLimiter.create(writeRate));
        this.scanLimit.put(tableName, scanLimit);
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
        final Class<?> clazz;
        try {
            clazz = Class.forName(credentialsClassName);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(VALIDATE_CREDENTIALS_CLASS_NAME, e);
        }
        final AWSCredentialsProvider credentialsProvider;
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
        if (null == constructorArgs) {
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
            instance = constructor.newInstance((Object[]) constructorArgs);
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new IllegalArgumentException("Cannot create new instance:" + clazz.getCanonicalName(), e);
        }
        return instance;
    }

    public String getPrefix() {
        return prefix;
    }

    public static final ThreadPoolExecutor getPoolFromNs(Configuration ns) {
        final int maxQueueSize = ns.getInt(Constants.CLIENT_EXECUTOR_QUEUE_MAX_LENGTH, Constants.CLIENT_EXECUTOR_QUEUE_MAX_LENGTH_DEFAULT);
        final ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("delegate-%d").build();
//begin adaptation of constructor at
//https://github.com/buka/titan/blob/master/src/main/java/com/thinkaurelius/titan/diskstorage/dynamodb/DynamoDBClient.java#L104
        final int maxPoolSize = ns.getInt(Constants.CLIENT_EXECUTOR_MAX_POOL_SIZE, Constants.CLIENT_EXECUTOR_MAX_POOL_SIZE_DEFAULT);
        final int corePoolSize = ns.getInt(Constants.CLIENT_EXECUTOR_CORE_POOL_SIZE, Constants.CLIENT_EXECUTOR_CORE_POOL_SIZE_DEFAULT);
        final long keepAlive = ns.getLong(Constants.CLIENT_EXECUTOR_KEEP_ALIVE, Constants.CLIENT_EXECUTOR_KEEP_ALIVE_DEFAULT);
        final ThreadPoolExecutor executor = new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAlive,
            TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(maxQueueSize), factory, new ThreadPoolExecutor.CallerRunsPolicy());
//end adaptation of constructor at
//https://github.com/buka/titan/blob/master/src/main/java/com/thinkaurelius/titan/diskstorage/dynamodb/DynamoDBClient.java#L104
        executor.allowCoreThreadTimeOut(false);
        executor.prestartAllCoreThreads();
        return executor;
    }
}
