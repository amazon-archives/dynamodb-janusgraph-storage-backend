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
package com.amazon.janusgraph.diskstorage.dynamodb.model;

import com.amazonaws.AmazonWebServiceRequest;

/**
 * To serialize mutations on one primary key in a DynamoDB table, need to be able to handle
 * UpdateItem and DeleteItem, as Items can be created but not deleted with an UpdateItem.
 * This wrapper interface allows the Storage Backend to serialize on any mutation, even if
 * it corresponds to deleting an Item.
 * @author Alexander Patrikalakis
 *
 * @param <T> UpdateItemRequest or DeleteItemRequest
 */
public interface MutateItemRequest<T extends AmazonWebServiceRequest>
{
    boolean isDelete();
    boolean isUpdate();
    T getRequest();
}
