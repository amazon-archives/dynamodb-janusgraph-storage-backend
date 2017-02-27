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

/**
 * Creates a store backend based on configuration.
 *
 * @author Matthew Sowders
 *
 */
public enum BackendDataModel {
    SINGLE {
        @Override
        public AwsStore createStoreBackend(DynamoDBStoreManager manager, String prefix, String name) {
            return new DynamoDBSingleRowStore(manager, prefix, name);
        }
    },
    MULTI {
        @Override
        public AwsStore createStoreBackend(DynamoDBStoreManager manager, String prefix, String name) {
            return new DynamoDBStore(manager, prefix, name);
        }
    };

    public abstract AwsStore createStoreBackend(DynamoDBStoreManager manager, String prefix, String name);

}
