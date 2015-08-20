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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import com.amazonaws.ClientConfiguration;
import com.thinkaurelius.titan.diskstorage.Backend;

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

    public static final String STORAGE_NS = "storage";
    public static final String DYNAMODB_NS = "dynamodb";
    public static final String STORES_NS = "stores";
    public static final String CLIENT_NS = "client";
    public static final String CLIENT_PROXY_NS = "proxy";
    public static final String CLIENT_SOCKET_NS = "socket";
    public static final String CLIENT_EXECUTOR_NS = "executor";
    public static final String CLIENT_CREDENTIALS_NS = "credentials";

    public static final String TITAN_DYNAMODB_MAX_SELF_THROTTLED_RETRIES = "max-self-throttled-retries";
    public static final long TITAN_DYNAMODB_MAX_SELF_THROTTLED_RETRIES_DEFAULT = 5L;

    public static final String TITAN_DYNAMODB_INTIAL_RETRY_MILLIS = "initial-retry-millis";
    public static final long TITAN_DYNAMODB_INTIAL_RETRY_MILLIS_DEFAULT = 25L;

    public static final String TITAN_DYNAMODB_CONTROL_PLANE_RATE = "control-plane-rate";
    public static final double TITAN_DYNAMODB_CONTROL_PLANE_RATE_DEFAULT = 10.0;

    public static final String TABLE_PREFIX = "prefix";
    public static final String TABLE_PREFIX_DEFAULT = "titan";

    public static final String ENABLE_PARALLEL_SCAN = "enable-parallel-scans";
    public static final boolean ENABLE_PARALLEL_SCAN_DEFAULT = false;

    public static final String METRICS_PREFIX = "metrics-prefix";
    public static final String METRICS_PREFIX_DEFAULT = "dynamodb";

//begin adaptation of the following block up until line 63
//https://github.com/buka/titan/blob/master/src/main/java/com/thinkaurelius/titan/diskstorage/dynamodb/DynamoDBClient.java#L29
    public static final String FORCE_CONSISTENT_READ = "force-consistent-read";
    public static final boolean FORCE_CONSISTENT_READ_DEFAULT = true;

    public static final String READ_CAPACITY = "capacity-read";
    public static final long READ_CAPACITY_DEFAULT = 750L;

    public static final String WRITE_CAPACITY = "capacity-write";
    public static final long WRITE_CAPACITY_DEFAULT = 750L;

    public static final String READ_RATE_LIMIT = "read-rate";
    public static final double READ_RATE_LIMIT_DEFAULT = 750.0;

    public static final String WRITE_RATE_LIMIT = "write-rate";
    public static final double WRITE_RATE_LIMIT_DEFAULT = 750.0;

    public static final String SCAN_LIMIT = "scan-limit";
    public static final int SCAN_LIMIT_DEFAULT = 10000;

    public static final String DATA_MODEL = "data-model";
    public static final String DATA_MODEL_DEFAULT = BackendDataModel.MULTI.name();

    public static final String CLIENT_CONN_TIMEOUT = "connection-timeout";
    public static final int CLIENT_CONN_TIMEOUT_DEFAULT = ClientConfiguration.DEFAULT_CONNECTION_TIMEOUT;

    public static final String CLIENT_CONN_TTL = "connection-ttl";
    public static final long CLIENT_CONN_TTL_DEFAULT = ClientConfiguration.DEFAULT_CONNECTION_TTL;

    public static final String CLIENT_MAX_CONN = "connection-max";
    public static final int CLIENT_MAX_CONN_DEFAULT = ClientConfiguration.DEFAULT_MAX_CONNECTIONS;

    public static final String CLIENT_MAX_ERROR_RETRY = "retry-error-max";
    public static final int CLIENT_MAX_ERROR_RETRY_DEFAULT = 0;

    public static final String CLIENT_USE_GZIP = "use-gzip";
    public static final boolean CLIENT_USE_GZIP_DEFAULT = ClientConfiguration.DEFAULT_USE_GZIP;

    public static final String CLIENT_USE_REAPER = "use-reaper";
    public static final boolean CLIENT_USE_REAPER_DEFAULT = ClientConfiguration.DEFAULT_USE_REAPER;

    public static final String CLIENT_USER_AGENT = "user-agent";
    public static final String CLIENT_USER_AGENT_DEFAULT = ClientConfiguration.DEFAULT_USER_AGENT;

    public static final String CLIENT_ENDPOINT = "endpoint";

    public static final String CLIENT_PROXY_DOMAIN = "domain";
    public static final String CLIENT_PROXY_WORKSTATION = "workstation";
    public static final String CLIENT_PROXY_HOST = "host";
    public static final String CLIENT_PROXY_PORT = "port";
    public static final String CLIENT_PROXY_USERNAME = "username";
    public static final String CLIENT_PROXY_PASSWORD = "password";

    public static final String CLIENT_SOCKET_BUFFER_SEND_HINT = "buffer-send-hint";
    public static final int CLIENT_SOCKET_BUFFER_SEND_HINT_DEFAULT = 1048576; // 1MB

    public static final String CLIENT_SOCKET_BUFFER_RECV_HINT = "buffer-recv-hint";
    public static final int CLIENT_SOCKET_BUFFER_RECV_HINT_DEFAULT = 1048576; // 1MB

    public static final String CLIENT_SOCKET_TIMEOUT = "timeout";
    public static final int CLIENT_SOCKET_TIMEOUT_DEFAULT = ClientConfiguration.DEFAULT_SOCKET_TIMEOUT;

    public static final String CLIENT_SOCKET_TCP_KEEP_ALIVE = "tcp-keep-alive";
    public static final boolean CLIENT_SOCKET_TCP_KEEP_ALIVE_DEFAULT = false;

    public static final String CLIENT_EXECUTOR_CORE_POOL_SIZE = "core-pool-size";
    public static final int CLIENT_EXECUTOR_CORE_POOL_SIZE_DEFAULT = Runtime.getRuntime().availableProcessors() * 2;

    public static final String CLIENT_EXECUTOR_MAX_POOL_SIZE = "max-pool-size";
    public static final int CLIENT_EXECUTOR_MAX_POOL_SIZE_DEFAULT = Runtime.getRuntime().availableProcessors() * 4;

    public static final String CLIENT_EXECUTOR_KEEP_ALIVE = "keep-alive";
    public static final int CLIENT_EXECUTOR_KEEP_ALIVE_DEFAULT = 60000;
//end adaptation of the following block up until line 63
//https://github.com/buka/titan/blob/master/src/main/java/com/thinkaurelius/titan/diskstorage/dynamodb/DynamoDBClient.java#L29

    public static final String CLIENT_EXECUTOR_QUEUE_MAX_LENGTH =  "max-queue-length";
    public static final int CLIENT_EXECUTOR_QUEUE_MAX_LENGTH_DEFAULT = 1024;

    public static final String CLIENT_EXECUTOR_MAX_CONCURRENT_OPERATIONS = "max-concurrent-operations";
    public static final int CLIENT_EXECUTOR_MAX_CONCURRENT_OPERATIONS_DEFAULT = 1;

    public static final String CREDENTIALS_CLASS_NAME = "class-name";
    public static final String CREDENTIALS_CLASS_NAME_DEFAULT = "com.amazonaws.auth.BasicAWSCredentials";

    public static final String CREDENTIALS_CONSTRUCTOR_ARGS = "constructor-args";
    public static final List<Object> CREDENTIALS_CONSTRUCTOR_ARGS_DEFAULT = Arrays.<Object>asList("accessKey", "secretKey");

    public static final List<String> BACKEND_STORE_NAMES = Arrays.asList(
            Backend.EDGEINDEX_STORE_NAME,
            Backend.EDGESTORE_NAME,
            Backend.ID_STORE_NAME,
            Backend.SYSTEM_PROPERTIES_STORE_NAME,
            Backend.VERTEXINDEX_STORE_NAME);

    // DynamoDB doesn't allow us to write empty binary values.
    public static final String EMPTY_BUFFER_PLACEHOLDER = "EMPTY";

}
