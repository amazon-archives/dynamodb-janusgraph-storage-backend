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

import java.util.concurrent.Callable;

import org.janusgraph.diskstorage.StaticBuffer;

import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;

/**
 * Worker class used to execute a GetItem request as a callable.
 * Used to allow multiqueries against SINGLE stores to execute across many threads.
 */
public class GetItemWorker implements Callable<GetItemResultWrapper> {

    private final GetItemRequest request;
    private final DynamoDbDelegate dynamoDbDelegate;
    private final StaticBuffer hashKey;

    public GetItemWorker(final StaticBuffer hashKey, final GetItemRequest request, final DynamoDbDelegate dynamoDbDelegate) {
        this.hashKey = hashKey;
        this.request = request;
        this.dynamoDbDelegate = dynamoDbDelegate;
    }

    @Override
    public GetItemResultWrapper call() throws Exception {
        final GetItemResult result = new ExponentialBackoff.GetItem(request, dynamoDbDelegate).runWithBackoff();
        return new GetItemResultWrapper(hashKey, result);
    }

}
