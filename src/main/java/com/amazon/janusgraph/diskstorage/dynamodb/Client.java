/*
 * Copyright 2014-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazon.janusgraph.diskstorage.dynamodb;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.RateLimiterCreator;

/**
 * Operations setting up the DynamoDB client.
 *
 * @author Matthew Sowders
 * @author Alexander Patrikalakis
 *
 */
public class Client {
    private static final String VALIDATE_CREDENTIALS_CLASS_NAME = "Must provide either an AWSCredentials or AWSCredentialsProvider fully qualified class name";
    private static final double DEFAULT_BURST_BUCKET_SIZE_IN_SECONDS = 300.0;

    private final Map<String, Long> capacityRead = new HashMap<>();
    private final Map<String, Long> capacityWrite = new HashMap<>();
    private final Map<String, BackendDataModel> dataModelMap = new HashMap<>();
    @Getter(AccessLevel.PACKAGE)
    private final boolean forceConsistentRead;
    @Getter(AccessLevel.PACKAGE)
    private final boolean enableParallelScan;
    private final Map<String, Integer> scanLimitMap = new HashMap<>();
    @Getter
    private final DynamoDbDelegate delegate;

    @Getter
    private final String prefix;

    public Client(final Configuration config) {
        final String credentialsClassName = config.get(Constants.DYNAMODB_CREDENTIALS_CLASS_NAME);
        final Class<?> clazz;
        try {
            clazz = Class.forName(credentialsClassName);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(VALIDATE_CREDENTIALS_CLASS_NAME, e);
        }

        final String[] credentialsConstructorArgsValues = config.get(Constants.DYNAMODB_CREDENTIALS_CONSTRUCTOR_ARGS);
        final List<String> filteredArgList = new ArrayList<>();
        for (Object obj : credentialsConstructorArgsValues) {
            final String str = obj.toString();
            if (!str.isEmpty()) {
                filteredArgList.add(str);
            }
        }

        final AWSCredentialsProvider credentialsProvider;
        if (AWSCredentials.class.isAssignableFrom(clazz)) {
            final AWSCredentials credentials = createCredentials(clazz, filteredArgList.toArray(new String[filteredArgList.size()]));
            credentialsProvider = new AWSStaticCredentialsProvider(credentials);
        } else if (AWSCredentialsProvider.class.isAssignableFrom(clazz)) {
            credentialsProvider = createCredentialsProvider(clazz, credentialsConstructorArgsValues);
        } else {
            throw new IllegalArgumentException(VALIDATE_CREDENTIALS_CLASS_NAME);
        }
//begin adaptation of constructor at
//https://github.com/buka/titan/blob/master/src/main/java/com/thinkaurelius/titan/diskstorage/dynamodb/DynamoDBClient.java#L77
        final ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.withConnectionTimeout(config.get(Constants.DYNAMODB_CLIENT_CONN_TIMEOUT))
                .withConnectionTTL(config.get(Constants.DYNAMODB_CLIENT_CONN_TTL))
                .withMaxConnections(config.get(Constants.DYNAMODB_CLIENT_MAX_CONN))
                .withMaxErrorRetry(config.get(Constants.DYNAMODB_CLIENT_MAX_ERROR_RETRY))
                .withGzip(config.get(Constants.DYNAMODB_CLIENT_USE_GZIP))
                .withReaper(config.get(Constants.DYNAMODB_CLIENT_USE_REAPER))
                .withUserAgentSuffix(config.get(Constants.DYNAMODB_CLIENT_USER_AGENT))
                .withSocketTimeout(config.get(Constants.DYNAMODB_CLIENT_SOCKET_TIMEOUT))
                .withSocketBufferSizeHints(
                        config.get(Constants.DYNAMODB_CLIENT_SOCKET_BUFFER_SEND_HINT),
                        config.get(Constants.DYNAMODB_CLIENT_SOCKET_BUFFER_RECV_HINT))
                .withProxyDomain(config.get(Constants.DYNAMODB_CLIENT_PROXY_DOMAIN))
                .withProxyWorkstation(config.get(Constants.DYNAMODB_CLIENT_PROXY_WORKSTATION))
                .withProxyHost(config.get(Constants.DYNAMODB_CLIENT_PROXY_HOST))
                .withProxyPort(config.get(Constants.DYNAMODB_CLIENT_PROXY_PORT))
                .withProxyUsername(config.get(Constants.DYNAMODB_CLIENT_PROXY_USERNAME))
                .withProxyPassword(config.get(Constants.DYNAMODB_CLIENT_PROXY_PASSWORD));
        forceConsistentRead = config.get(Constants.DYNAMODB_FORCE_CONSISTENT_READ);
//end adaptation of constructor at
//https://github.com/buka/titan/blob/master/src/main/java/com/thinkaurelius/titan/diskstorage/dynamodb/DynamoDBClient.java#L77
        enableParallelScan = config.get(Constants.DYNAMODB_ENABLE_PARALLEL_SCAN);
        prefix = config.get(Constants.DYNAMODB_TABLE_PREFIX);
        final String metricsPrefix = config.get(Constants.DYNAMODB_METRICS_PREFIX);

        final long maxRetries = config.get(Constants.DYNAMODB_MAX_SELF_THROTTLED_RETRIES);
        Preconditions.checkArgument(maxRetries >= 0,
            Constants.DYNAMODB_MAX_SELF_THROTTLED_RETRIES.getName() + " must be at least 0");

        final long retryMillis = config.get(Constants.DYNAMODB_INITIAL_RETRY_MILLIS);
        Preconditions.checkArgument(retryMillis > 0,
            Constants.DYNAMODB_INITIAL_RETRY_MILLIS.getName() + " must be at least 1");

        final double controlPlaneRate = config.get(Constants.DYNAMODB_CONTROL_PLANE_RATE);
        Preconditions.checkArgument(controlPlaneRate > 0,
            "must have a positive control plane rate");
        final RateLimiter controlPlaneRateLimiter = RateLimiterCreator.createBurstingLimiter(controlPlaneRate,
                DEFAULT_BURST_BUCKET_SIZE_IN_SECONDS);

        final Map<String, RateLimiter> readRateLimit = new HashMap<>();
        final Map<String, RateLimiter> writeRateLimit = new HashMap<>();

        final Set<String> storeNames = new HashSet<>(Constants.REQUIRED_BACKEND_STORES);
        storeNames.add(config.get(GraphDatabaseConfiguration.IDS_STORE_NAME));
        storeNames.addAll(config.getContainedNamespaces(Constants.DYNAMODB_STORES_NAMESPACE));
        storeNames.forEach(storeName -> setupStore(config, readRateLimit, writeRateLimit, storeName));

        delegate = new DynamoDbDelegate(JanusGraphConfigUtil.getNullableConfigValue(config, Constants.DYNAMODB_CLIENT_ENDPOINT),
                JanusGraphConfigUtil.getNullableConfigValue(config, Constants.DYNAMODB_CLIENT_SIGNING_REGION),
                credentialsProvider,
            clientConfig, config, readRateLimit, writeRateLimit, maxRetries, retryMillis, prefix, metricsPrefix, controlPlaneRateLimiter);
    }

    private void setupStore(final Configuration config,
        final Map<String, RateLimiter> readRateLimit, final Map<String, RateLimiter> writeRateLimit, final String store) {

        final String dataModel = config.get(Constants.STORES_DATA_MODEL, store);
        final int scanLimit = config.get(Constants.STORES_SCAN_LIMIT, store);
        final long readCapacity = config.get(Constants.STORES_INITIAL_CAPACITY_READ, store);
        final long writeCapacity = config.get(Constants.STORES_INITIAL_CAPACITY_WRITE, store);
        final double readRate = config.get(Constants.STORES_READ_RATE_LIMIT, store);
        final double writeRate = config.get(Constants.STORES_WRITE_RATE_LIMIT, store);

        final String actualTableName = prefix + "_" + store;

        this.dataModelMap.put(store, BackendDataModel.valueOf(dataModel));
        this.capacityRead.put(actualTableName, readCapacity);
        this.capacityWrite.put(actualTableName, writeCapacity);
        readRateLimit.put(actualTableName, RateLimiterCreator.createBurstingLimiter(readRate, DEFAULT_BURST_BUCKET_SIZE_IN_SECONDS));
        writeRateLimit.put(actualTableName, RateLimiterCreator.createBurstingLimiter(writeRate, DEFAULT_BURST_BUCKET_SIZE_IN_SECONDS));
        this.scanLimitMap.put(actualTableName, scanLimit);
    }

    long readCapacity(@NonNull final String tableName) {
        return capacityRead.get(tableName);
    }

    long writeCapacity(@NonNull final String tableName) {
        return capacityWrite.get(tableName);
    }

    BackendDataModel dataModel(final String storeName) {
        return dataModelMap.get(storeName);
    }

    int scanLimit(final String tableName) {
        return scanLimitMap.get(tableName);
    }

    private static AWSCredentialsProvider createCredentialsProvider(final Class<?> clazz, final String[] credentialsProviderConstructorArgs) {
        return (AWSCredentialsProvider) createInstance(clazz, credentialsProviderConstructorArgs);
    }

    private static AWSCredentials createCredentials(final Class<?> clazz, final String[] credentialsConstructorArgs) {
        return (AWSCredentials) createInstance(clazz, credentialsConstructorArgs);
    }

    private static Object createInstance(final Class<?> clazz, final String[] constructorArgs) {
        final Class<?>[] constructorTypes;
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
        final Constructor<?> constructor;
        try {
            constructor = clazz.getConstructor(constructorTypes);
        } catch (NoSuchMethodException | SecurityException e) {
            throw new IllegalArgumentException("Cannot access constructor:" + clazz.getCanonicalName() + "(" + constructorTypes.length + ")", e);
        }
        final Object instance;
        try {
            instance = constructor.newInstance((Object[]) actualArgs);
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new IllegalArgumentException("Cannot create new instance:" + clazz.getCanonicalName(), e);
        }
        return instance;
    }

}
