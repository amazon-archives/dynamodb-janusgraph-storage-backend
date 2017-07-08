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
package com.amazon.janusgraph.diskstorage.dynamodb.iterator;

import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Holds the request and response for one "page" of a DynamoDB scan.
 *
 * @author Michael Rodaitis
 */
@Getter
@RequiredArgsConstructor
public class ScanContext {

    private final ScanRequest scanRequest;
    private final ScanResult scanResult;

    public boolean isFirstResult() {
        return scanRequest.getExclusiveStartKey() == null || scanRequest.getExclusiveStartKey().isEmpty();
    }
}
