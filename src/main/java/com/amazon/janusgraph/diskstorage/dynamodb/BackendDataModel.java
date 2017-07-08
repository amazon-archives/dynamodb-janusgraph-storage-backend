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

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Creates a store backend based on configuration.
 *
 * @author Matthew Sowders
 *
 */
@RequiredArgsConstructor
public enum BackendDataModel {
    SINGLE("Single") {
        @Override
        public AwsStore createStoreBackend(final DynamoDBStoreManager manager, final String prefix, final String name) {
            return new DynamoDbSingleRowStore(manager, prefix, name);
        }
    },
    MULTI("Multiple") {
        @Override
        public AwsStore createStoreBackend(final DynamoDBStoreManager manager, final String prefix, final String name) {
            return new DynamoDbStore(manager, prefix, name);
        }
    };

    @Getter
    private final String camelCaseName;

    public abstract AwsStore createStoreBackend(DynamoDBStoreManager manager, String prefix, String name);

}
