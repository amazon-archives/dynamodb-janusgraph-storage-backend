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

import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;

/**
 * DeleteRequestWrapper is the MutateItemRequest implementation that chooses DeleteItemRequest
 * @author Alexander Patrikalakis
 *
 */
public class DeleteRequestWrapper implements MutateItemRequest<DeleteItemRequest>
{
    private final DeleteItemRequest request;
    public DeleteRequestWrapper(DeleteItemRequest request) {
        this.request = request;
    }
    @Override
    public boolean isDelete() {
        return true;
    }

    @Override
    public boolean isUpdate() {
        return false;
    }

    @Override
    public DeleteItemRequest getRequest(){
        return request;
    }
}
