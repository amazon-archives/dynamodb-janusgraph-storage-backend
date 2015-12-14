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
package com.amazon.titan.diskstorage.dynamodb;

import java.util.concurrent.Callable;

import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;

/**
 * Worker class used to execute a GetItem request as a callable.
 * Used to allow multiqueries against SINGLE stores to execute across many threads.
 */
public class GetItemWorker implements Callable<GetItemResultWrapper> {

    private final GetItemRequest request;
    private final DynamoDBDelegate dynamoDBDelegate;
    private final StaticBuffer hashKey;

    public GetItemWorker(StaticBuffer hashKey, GetItemRequest request, DynamoDBDelegate dynamoDBDelegate) {
        this.hashKey = hashKey;
        this.request = request;
        this.dynamoDBDelegate = dynamoDBDelegate;
    }

    @Override
    public GetItemResultWrapper call() throws Exception {
        final GetItemResult result = new ExponentialBackoff.GetItem(request, dynamoDBDelegate).runWithBackoff();
        return new GetItemResultWrapper(hashKey, result);
    }

}
