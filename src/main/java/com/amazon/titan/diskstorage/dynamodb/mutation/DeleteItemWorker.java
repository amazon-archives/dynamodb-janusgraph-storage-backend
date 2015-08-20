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
package com.amazon.titan.diskstorage.dynamodb.mutation;

import com.amazon.titan.diskstorage.dynamodb.DynamoDBDelegate;
import com.amazon.titan.diskstorage.dynamodb.ExponentialBackoff.DeleteItem;
import com.amazon.titan.diskstorage.dynamodb.mutation.MutateWorker;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.thinkaurelius.titan.diskstorage.StorageException;

/**
 *
 * @author Alexander Patrikalakis
 *
 */
public class DeleteItemWorker implements MutateWorker {

    private DeleteItemRequest deleteItemRequest;
    private DynamoDBDelegate dynamoDBDelegate;

    public DeleteItemWorker(DeleteItemRequest deleteItemRequest, DynamoDBDelegate dynamoDBDelegate) {
        this.deleteItemRequest = deleteItemRequest;
        this.dynamoDBDelegate = dynamoDBDelegate;
    }

    @Override
    public Void call() throws StorageException {
        new DeleteItem(deleteItemRequest, dynamoDBDelegate).runWithBackoff();

        // void
        return null;
    }
}
