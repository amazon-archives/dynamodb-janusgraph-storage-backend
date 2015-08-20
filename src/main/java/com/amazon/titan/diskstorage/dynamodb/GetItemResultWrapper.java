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

import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;

/**
 * Titan's interface for multiqueries requires us to map GetItem results to a StaticBuffer.
 * This class is used to simplify that process by holding both relevant pieces of data in a POJO.
 */
public class GetItemResultWrapper {

    private final StaticBuffer titanKey;
    private final GetItemResult dynamoDBResult;

    public GetItemResultWrapper(StaticBuffer titanKey, GetItemResult dynamoDBResult) {
        this.titanKey = titanKey;
        this.dynamoDBResult = dynamoDBResult;
    }

    public StaticBuffer getTitanKey() {
        return titanKey;
    }

    public GetItemResult getDynamoDBResult() {
        return dynamoDBResult;
    }

}
