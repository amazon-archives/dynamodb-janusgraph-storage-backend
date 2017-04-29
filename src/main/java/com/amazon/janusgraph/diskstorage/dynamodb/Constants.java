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
package com.amazon.janusgraph.diskstorage.dynamodb;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import com.amazonaws.ClientConfiguration;
import com.google.common.base.Predicates;
import org.janusgraph.diskstorage.Backend;
import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;

/**
 * Constants for the DynamoDB backend.
 *
 * @author Matthew Sowders
 *
 */
public class Constants {
//begin adaptation of
//https://github.com/buka/titan/blob/master/src/main/java/com/thinkaurelius/titan/diskstorage/dynamodb/DynamoDBClient.java#L26
    public static final String TITAN_VALUE = "v";
    public static final int TITAN_VALUE_BYTES = TITAN_VALUE.getBytes(StandardCharsets.UTF_8).length;
    public static final String TITAN_HASH_KEY = "hk";
    public static final int TITAN_HASH_KEY_BYTES = TITAN_HASH_KEY.getBytes(StandardCharsets.UTF_8).length;
    public static final String TITAN_RANGE_KEY = "rk";
//end adaptation of
//https://github.com/buka/titan/blob/master/src/main/java/com/thinkaurelius/titan/diskstorage/dynamodb/DynamoDBClient.java#L26
    public static final int TITAN_RANGE_KEY_BYTES = TITAN_RANGE_KEY.getBytes(StandardCharsets.UTF_8).length;
    public static final String HEX_PREFIX = "0x";

    public static final List<String> REQUIRED_BACKEND_STORES = Arrays.asList(Backend.EDGESTORE_NAME, //
        Backend.INDEXSTORE_NAME, //
        Backend.ID_STORE_NAME, //
        Backend.SYSTEM_TX_LOG_NAME, //
        Backend.SYSTEM_MGMT_LOG_NAME, //
        GraphDatabaseConfiguration.SYSTEM_PROPERTIES_STORE_NAME);

    public static final ConfigNamespace DYNAMODB_CONFIGURATION_NAMESPACE =
        new ConfigNamespace(GraphDatabaseConfiguration.STORAGE_NS, "dynamodb",
            "DynamoDB storage options", false /*isUmbrella*/);
    public static final ConfigNamespace DYNAMODB_STORES_NAMESPACE =
        new ConfigNamespace(DYNAMODB_CONFIGURATION_NAMESPACE, "stores",
            "DynamoDB KCV store options", true /*isUmbrella*/);
    public static final ConfigNamespace DYNAMODB_CLIENT_NAMESPACE =
        new ConfigNamespace(DYNAMODB_CONFIGURATION_NAMESPACE,
        "client", "DynamoDB client options", false /*isUmbrella*/);
    public static final ConfigNamespace DYNAMODB_CLIENT_PROXY_NAMESPACE =
        new ConfigNamespace(DYNAMODB_CLIENT_NAMESPACE,
        "proxy", "DynamoDB client proxy options", false /*isUmbrella*/);
    public static final ConfigNamespace DYNAMODB_CLIENT_SOCKET_NAMESPACE =
        new ConfigNamespace(DYNAMODB_CLIENT_NAMESPACE, "socket", "DynamoDB client socket options", false /*isUmbrella*/);
    public static final ConfigNamespace DYNAMODB_CLIENT_EXECUTOR_NAMESPACE =
        new ConfigNamespace(DYNAMODB_CLIENT_NAMESPACE,
        "executor", "DynamoDB client executor options", false /*isUmbrella*/);
    public static final ConfigNamespace DYNAMODB_CLIENT_CREDENTIALS_NAMESPACE =
        new ConfigNamespace(DYNAMODB_CLIENT_NAMESPACE, "credentials", "DynamoDB client credentials options",
            false /*isUmbrella*/);

    public static final ConfigOption<String> DYNAMODB_TABLE_PREFIX = new ConfigOption<>(DYNAMODB_CONFIGURATION_NAMESPACE,
        "prefix", "A prefix to put before the JanusGraph table name. "
        + "This allows clients to have multiple graphs on the same AWS DynamoDB account.",
        ConfigOption.Type.FIXED, "jg");
    public static final ConfigOption<String> DYNAMODB_METRICS_PREFIX = new ConfigOption<>(DYNAMODB_CONFIGURATION_NAMESPACE, "metrics-prefix",
        "Prefix on the codahale metric names emitted by DynamoDBDelegate.",
        ConfigOption.Type.MASKABLE, "dynamodb");
    public static final ConfigOption<Boolean> DYNAMODB_ENABLE_PARALLEL_SCAN =
        new ConfigOption<>(DYNAMODB_CONFIGURATION_NAMESPACE, "enable-parallel-scans",
        "This feature enables scans to run in parallel, which should decrease the total blocking time "
            + "spent when iterating over large sets of vertices. "
            + "WARNING: while this feature is enabled Titan's OLAP libraries are NOT supported."
            + "The Fulgora implementations of OLAP rely on consistent scan orders across multiple scans, "
            + "which cannot be guaranteed when scans are run in parallel",
        ConfigOption.Type.MASKABLE, false);
    public static final ConfigOption<String> STORES_DATA_MODEL =
        new ConfigOption<>(Constants.DYNAMODB_STORES_NAMESPACE, "data-model",
        "SINGLE Means that all the values for a given key are put into a single DynamoDB item. "
            + "A SINGLE is efficient because all the updates for a single key can be done atomically. "
            + "However, the trade-off is that DynamoDB has a 400k limit per item so it cannot hold much data. "
            + "MULTI Means that each 'column' is used as a range key in DynamoDB so a key can span multiple items. "
            + "A MULTI implementation is slightly less efficient than SINGLE because it must use DynamoDB Query "
            + "rather than a direct lookup. It is HIGHLY recommended to use MULTI for edgestore unless your graph has "
            + "very low max degree.",
        ConfigOption.Type.FIXED, BackendDataModel.MULTI.name());
    public static final ConfigOption<Integer> STORES_SCAN_LIMIT =
        new ConfigOption<>(Constants.DYNAMODB_STORES_NAMESPACE, "scan-limit",
        "The maximum number of items to evaluate (not necessarily the number of matching items). "
            + "If DynamoDB processes the number of items up to the limit while processing the results, it stops "
            + "the operation and returns the matching values up to that point, and a key in LastEvaluatedKey to apply "
            + "in a subsequent operation, so that you can pick up where you left off. Also, if the processed data set "
            + "size exceeds 1 MB before DynamoDB reaches this limit, it stops the operation and returns the matching "
            + "values up to the limit, and a key in LastEvaluatedKey to apply in a subsequent operation to continue "
            + "the operation.",
        ConfigOption.Type.MASKABLE, 10000);

//begin adaptation of the following block up until line 63
//https://github.com/buka/titan/blob/master/src/main/java/com/thinkaurelius/titan/diskstorage/dynamodb/DynamoDBClient.java#L29
    public static final ConfigOption<Boolean> DYNAMODB_FORCE_CONSISTENT_READ =
    new ConfigOption<>(DYNAMODB_CONFIGURATION_NAMESPACE, "force-consistent-read",
        "This feature sets the force consistent read property on dynamodb calls.",
        ConfigOption.Type.MASKABLE, true);
    public static final ConfigOption<Long> STORES_CAPACITY_READ =
        new ConfigOption<>(Constants.DYNAMODB_STORES_NAMESPACE, "capacity-read",
        "Define the initial read capacity for a given dynamodb table.",
        ConfigOption.Type.GLOBAL, 4L);
    public static final ConfigOption<Long> STORES_CAPACITY_WRITE =
        new ConfigOption<>(Constants.DYNAMODB_STORES_NAMESPACE, "capacity-write",
        "Define the initial write capacity for a given dynamodb table.",
        ConfigOption.Type.GLOBAL, 4L);
    public static final ConfigOption<Double> STORES_READ_RATE_LIMIT =
        new ConfigOption<>(Constants.DYNAMODB_STORES_NAMESPACE, "read-rate",
        "The max number of reads per second.",
        ConfigOption.Type.MASKABLE, 4.0);
    public static final ConfigOption<Double> STORES_WRITE_RATE_LIMIT =
        new ConfigOption<>(Constants.DYNAMODB_STORES_NAMESPACE, "write-rate",
        "Used to throttle write rate of given table. The max number of writes per second.",
        ConfigOption.Type.MASKABLE, 4.0);
    public static final ConfigOption<Integer> DYNAMODB_CLIENT_CONN_TIMEOUT =
        new ConfigOption<>(DYNAMODB_CLIENT_NAMESPACE, "connection-timeout",
        "The amount of time to wait (in milliseconds) when initially establishing a connection before giving up and timing out.", //
        ConfigOption.Type.MASKABLE, ClientConfiguration.DEFAULT_CONNECTION_TIMEOUT);
    public static final ConfigOption<Long> DYNAMODB_CLIENT_CONN_TTL =
        new ConfigOption<>(DYNAMODB_CLIENT_NAMESPACE, "connection-ttl",
        "The expiration time (in milliseconds) for a connection in the connection pool.",
        ConfigOption.Type.MASKABLE, ClientConfiguration.DEFAULT_CONNECTION_TTL);
    public static final ConfigOption<Integer> DYNAMODB_CLIENT_MAX_CONN =
        new ConfigOption<>(DYNAMODB_CLIENT_NAMESPACE, "connection-max",
        "The maximum number of allowed open HTTP connections.",
        ConfigOption.Type.MASKABLE, ClientConfiguration.DEFAULT_MAX_CONNECTIONS);
    public static final ConfigOption<Integer> DYNAMODB_CLIENT_MAX_ERROR_RETRY =
        new ConfigOption<>(DYNAMODB_CLIENT_NAMESPACE, "retry-error-max",
        "The maximum number of retry attempts for failed retryable "
            + "requests (ex: 5xx error responses from services).",
        ConfigOption.Type.MASKABLE, 0);
    public static final ConfigOption<Boolean> DYNAMODB_CLIENT_USE_GZIP =
        new ConfigOption<>(DYNAMODB_CLIENT_NAMESPACE, "use-gzip",
        "Sets whether gzip compression should be used.",
        ConfigOption.Type.MASKABLE, ClientConfiguration.DEFAULT_USE_GZIP);
    public static final ConfigOption<Boolean> DYNAMODB_CLIENT_USE_REAPER =
        new ConfigOption<>(DYNAMODB_CLIENT_NAMESPACE, "use-reaper",
        "Sets whether the IdleConnectionReaper is to be started as a daemon thread.",
        ConfigOption.Type.MASKABLE, ClientConfiguration.DEFAULT_USE_REAPER);
    public static final ConfigOption<String> DYNAMODB_CLIENT_USER_AGENT =
        new ConfigOption<>(DYNAMODB_CLIENT_NAMESPACE, "user-agent",
        "The HTTP user agent header to send with all requests.",
        ConfigOption.Type.MASKABLE, ClientConfiguration.DEFAULT_USER_AGENT);
    public static final ConfigOption<String> DYNAMODB_CLIENT_ENDPOINT =
        new ConfigOption<>(DYNAMODB_CLIENT_NAMESPACE, "endpoint",
        "Sets the service endpoint to use for connecting to DynamoDB.",
        ConfigOption.Type.LOCAL, String.class);
    public static final ConfigOption<String> DYNAMODB_CLIENT_SIGNING_REGION =
        new ConfigOption<>(DYNAMODB_CLIENT_NAMESPACE, "signing-region",
        "Sets the signing region to use for signing requests to DynamoDB.",
        ConfigOption.Type.LOCAL, String.class);

    public static final ConfigOption<String> DYNAMODB_CLIENT_PROXY_DOMAIN =
        new ConfigOption<>(DYNAMODB_CLIENT_PROXY_NAMESPACE, "domain",
        "The optional Windows domain name for configuration an NTLM proxy.",
        ConfigOption.Type.MASKABLE, "", Predicates.alwaysTrue());
    public static final ConfigOption<String> DYNAMODB_CLIENT_PROXY_WORKSTATION =
        new ConfigOption<>(DYNAMODB_CLIENT_PROXY_NAMESPACE, "workstation",
        "The optional Windows workstation name for configuring NTLM proxy support.",
        ConfigOption.Type.MASKABLE, "", Predicates.alwaysTrue());
    public static final ConfigOption<String> DYNAMODB_CLIENT_PROXY_HOST =
        new ConfigOption<>(DYNAMODB_CLIENT_PROXY_NAMESPACE, "host",
        "The optional proxy host the client will connect through.",
        ConfigOption.Type.MASKABLE, "", Predicates.alwaysTrue());
    public static final ConfigOption<Integer> DYNAMODB_CLIENT_PROXY_PORT =
        new ConfigOption<>(DYNAMODB_CLIENT_PROXY_NAMESPACE, "port",
        "The optional proxy port the client will connect through.",
        ConfigOption.Type.MASKABLE, 0, Predicates.alwaysTrue());
    public static final ConfigOption<String> DYNAMODB_CLIENT_PROXY_USERNAME =
        new ConfigOption<>(DYNAMODB_CLIENT_PROXY_NAMESPACE, "username",
        "The optional proxy user name to use if connecting through a proxy.",
        ConfigOption.Type.MASKABLE, "", Predicates.alwaysTrue());
    public static final ConfigOption<String> DYNAMODB_CLIENT_PROXY_PASSWORD =
        new ConfigOption<>(DYNAMODB_CLIENT_PROXY_NAMESPACE, "password",
        "The optional proxy password to use when connecting through a proxy.",
        ConfigOption.Type.MASKABLE, "", Predicates.alwaysTrue());

    public static final ConfigOption<Integer> DYNAMODB_CLIENT_SOCKET_BUFFER_SEND_HINT =
        new ConfigOption<>(DYNAMODB_CLIENT_SOCKET_NAMESPACE, "buffer-send-hint",
        "The optional size hint (in bytes) for the low level TCP send buffer.",
        ConfigOption.Type.MASKABLE, 1048576);
    public static final ConfigOption<Integer> DYNAMODB_CLIENT_SOCKET_BUFFER_RECV_HINT =
        new ConfigOption<>(DYNAMODB_CLIENT_SOCKET_NAMESPACE, "buffer-recv-hint",
        "The optional size hints (in bytes) for the low level TCP receive buffer.",
        ConfigOption.Type.MASKABLE, 1048576);
    public static final ConfigOption<Integer> DYNAMODB_CLIENT_SOCKET_TIMEOUT =
        new ConfigOption<>(DYNAMODB_CLIENT_SOCKET_NAMESPACE, "timeout",
        "The amount of time to wait (in milliseconds) for data to be transfered over an established, "
            + "open connection before the connection times out and is closed.",
        ConfigOption.Type.MASKABLE, ClientConfiguration.DEFAULT_SOCKET_TIMEOUT);
    public static final ConfigOption<Boolean> DYNAMODB_CLIENT_SOCKET_TCP_KEEP_ALIVE =
        new ConfigOption<>(DYNAMODB_CLIENT_SOCKET_NAMESPACE, "tcp-keep-alive",
        "Sets whether or not to enable TCP KeepAlive support at the socket level. Not used at the moment.",
        ConfigOption.Type.MASKABLE, false);

    public static final ConfigOption<Integer> DYNAMODB_CLIENT_EXECUTOR_CORE_POOL_SIZE =
        new ConfigOption<>(DYNAMODB_CLIENT_EXECUTOR_NAMESPACE, "core-pool-size",
        "The core number of threads for the DynamoDB async client.",
        ConfigOption.Type.MASKABLE, Runtime.getRuntime().availableProcessors() * 2);
    public static final ConfigOption<Integer> DYNAMODB_CLIENT_EXECUTOR_MAX_POOL_SIZE =
        new ConfigOption<>(DYNAMODB_CLIENT_EXECUTOR_NAMESPACE, "max-pool-size",
        "The maximum allowed number of threads for the DynamoDB async client.",
        ConfigOption.Type.MASKABLE, Runtime.getRuntime().availableProcessors() * 4);
    public static final ConfigOption<Long> DYNAMODB_CLIENT_EXECUTOR_KEEP_ALIVE =
        new ConfigOption<>(DYNAMODB_CLIENT_EXECUTOR_NAMESPACE, "keep-alive",
        "The time limit for which threads may remain idle before being terminated for the DynamoDB async client.",
        ConfigOption.Type.MASKABLE, 60000L);
//end adaptation of the following block up until line 63
//https://github.com/buka/titan/blob/master/src/main/java/com/thinkaurelius/titan/diskstorage/dynamodb/DynamoDBClient.java#L29

    public static final ConfigOption<Integer> DYNAMODB_CLIENT_EXECUTOR_QUEUE_MAX_LENGTH =
        new ConfigOption<>(DYNAMODB_CLIENT_EXECUTOR_NAMESPACE, "max-queue-length",
        "The maximum size of the executor queue before requests start getting run in the caller.",
        ConfigOption.Type.MASKABLE, 1024);
    public static final ConfigOption<Long> DYNAMODB_MAX_SELF_THROTTLED_RETRIES =
        new ConfigOption<>(DYNAMODB_CONFIGURATION_NAMESPACE, "max-self-throttled-retries",
        "The max number of retries to use when DynamoDB throws temporary failure exceptions",
        ConfigOption.Type.MASKABLE, 60L);
    public static final ConfigOption<Long> DYNAMODB_INITIAL_RETRY_MILLIS =
        new ConfigOption<>(DYNAMODB_CONFIGURATION_NAMESPACE, "initial-retry-millis",
        "The initial retry time (in milliseconds) to use during exponential backoff between DynamoDB requests",
        ConfigOption.Type.MASKABLE, 25L);
    public static final ConfigOption<Double> DYNAMODB_CONTROL_PLANE_RATE =
        new ConfigOption<>(DYNAMODB_CONFIGURATION_NAMESPACE, "control-plane-rate",
        "The maximum rate at which control plane requests (CreateTable, UpdateTable, DeleteTable, ListTables, "
            + "DescribeTable) are issued.",
        ConfigOption.Type.MASKABLE, 10.0);

    public static final ConfigOption<Integer> DYNAMODB_CLIENT_EXECUTOR_MAX_CONCURRENT_OPERATIONS =
        new ConfigOption<>(DYNAMODB_CLIENT_EXECUTOR_NAMESPACE, "max-concurrent-operations",
        "The expected number of threads expected to be using a single TitanGraph instance. "
            + "Used to allocate threads to batch operations", //
        ConfigOption.Type.MASKABLE, 1);

    public static final ConfigOption<String> DYNAMODB_CREDENTIALS_CLASS_NAME =
        new ConfigOption<>(DYNAMODB_CLIENT_CREDENTIALS_NAMESPACE, "class-name",
        "Specify the fully qualified class that implements AWSCredentialsProvider or AWSCredentials.",
        ConfigOption.Type.MASKABLE, "com.amazonaws.auth.BasicAWSCredentials");
    public static final ConfigOption<String[]> DYNAMODB_CREDENTIALS_CONSTRUCTOR_ARGS =
        new ConfigOption<>(DYNAMODB_CLIENT_CREDENTIALS_NAMESPACE, "constructor-args",
        "Comma separated list of strings to pass to the credentials constructor.",
        ConfigOption.Type.MASKABLE, new String[] { "accessKey", "secretKey" });

    // DynamoDB doesn't allow empty binary values.
    public static final String EMPTY_BUFFER_PLACEHOLDER = "EMPTY";

}
