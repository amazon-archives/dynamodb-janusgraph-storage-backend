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
package com.amazon.janusgraph.diskstorage.dynamodb.mutation;

import org.janusgraph.diskstorage.BackendException;

import com.amazon.janusgraph.diskstorage.dynamodb.DynamoDbDelegate;
import com.amazon.janusgraph.diskstorage.dynamodb.ExponentialBackoff.UpdateItem;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;

import lombok.RequiredArgsConstructor;

/**
 *
 * @author Alexander Patrikalakis
 *
 */
@RequiredArgsConstructor
public class UpdateItemWorker implements MutateWorker {

    private final UpdateItemRequest updateItemRequest;
    private final DynamoDbDelegate dynamoDbDelegate;

    @Override
    public Void call() throws BackendException {
        final UpdateItem updateBackoff = new UpdateItem(updateItemRequest, dynamoDbDelegate);
        updateBackoff.runWithBackoff();

        // void
        return null;
    }
}
