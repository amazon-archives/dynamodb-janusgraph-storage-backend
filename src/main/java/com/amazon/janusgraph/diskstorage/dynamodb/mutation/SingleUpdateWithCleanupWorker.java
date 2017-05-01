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

import java.util.Map;

import org.janusgraph.diskstorage.BackendException;

import com.amazon.janusgraph.diskstorage.dynamodb.Constants;
import com.amazon.janusgraph.diskstorage.dynamodb.DynamoDBDelegate;
import com.amazon.janusgraph.diskstorage.dynamodb.ExponentialBackoff.DeleteItem;
import com.amazon.janusgraph.diskstorage.dynamodb.ExponentialBackoff.UpdateItem;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;

/**
 *
 * @author Alexander Patrikalakis
 * @author Michael Rodaitis
 *
 */
public class SingleUpdateWithCleanupWorker implements MutateWorker {

    private static final int ATTRIBUTES_IN_EMPTY_SINGLE_ITEM = 1;

    private UpdateItemRequest updateItemRequest;
    private DynamoDBDelegate dynamoDBDelegate;

    public SingleUpdateWithCleanupWorker(UpdateItemRequest updateItemRequest, DynamoDBDelegate dynamoDBDelegate) {
        this.updateItemRequest = updateItemRequest;
        this.dynamoDBDelegate = dynamoDBDelegate;
    }

    @Override
    public Void call() throws BackendException {

        UpdateItem updateBackoff = new UpdateItem(updateItemRequest, dynamoDBDelegate);
        UpdateItemResult result = updateBackoff.runWithBackoff();


        final Map<String, AttributeValue> item = result.getAttributes();

        if (item == null) {
            // bail
            return null;
        }

        // If the record has no Titan columns left after deletions occur, then just delete the record
        if (item.containsKey(Constants.TITAN_HASH_KEY) && item.size() == ATTRIBUTES_IN_EMPTY_SINGLE_ITEM) {
            DeleteItem deleteBackoff = new DeleteItem(new DeleteItemRequest().withTableName(updateItemRequest.getTableName())
                                                                             .withKey(updateItemRequest.getKey()),
                                                      dynamoDBDelegate);
            deleteBackoff.runWithBackoff();
        }

        // void
        return null;
    }
}
