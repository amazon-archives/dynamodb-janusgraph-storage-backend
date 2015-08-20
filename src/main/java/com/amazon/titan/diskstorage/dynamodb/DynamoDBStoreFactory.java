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

import com.thinkaurelius.titan.diskstorage.BackendException;

/**
 * Creates a backend store for a given table name.
 *
 * @author Matthew Sowders
 *
 */
public interface DynamoDBStoreFactory {
    /**
     * Creates a backend store for a given table name.
     *
     * @param prefix
     * @param name
     * @return
     * @throws BackendException
     */
    AwsStore create(DynamoDBStoreManager manager, String prefix, String name) throws BackendException;

    /**
     * Gets all stores created by this factory.
     *
     * @return
     */
    Iterable<AwsStore> getAllStores();

    /**
     * Gets backend store for store name.
     *
     * @param store
     * @return
     */
    AwsStore getStore(String store);

}
