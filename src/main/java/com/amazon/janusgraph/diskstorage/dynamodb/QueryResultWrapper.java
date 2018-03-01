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

import org.janusgraph.diskstorage.StaticBuffer;

import com.amazonaws.services.dynamodbv2.model.QueryResult;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * The QueryResultWrapper class associates the JanusGraph key that was used to create the QueryRequest
 * resulting in the enclosed QueryResult.
 *
 * @author Alexander Patrikalakis
 */
@RequiredArgsConstructor
public class QueryResultWrapper {

    @Getter(AccessLevel.PACKAGE)
    private final StaticBuffer titanKey;
    @Getter
    private final QueryResult dynamoDBResult;
}
