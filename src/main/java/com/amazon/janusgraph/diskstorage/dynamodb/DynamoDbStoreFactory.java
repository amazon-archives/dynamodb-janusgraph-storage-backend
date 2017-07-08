/*
 * Copyright 2014-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import org.janusgraph.diskstorage.BackendException;

/**
 * Creates a backend store for a given table name.
 *
 * @author Matthew Sowders
 *
 */
public interface DynamoDbStoreFactory {
    /**
     * Creates a backend store for a given table name.
     *
     * @param prefix the prefix of the table name. For example if prefix was foo and name was bar,
     * the full table name would be foo_bar. The prefix is shared by all stores created by a factory.
     * @param name the name of the KCVStore, without the prefix.
     * @return a KCVStore with the given name and table prefix
     * @throws BackendException
     */
    AwsStore create(DynamoDBStoreManager manager, String prefix, String name) throws BackendException;

    /**
     * Gets all stores created by this factory.
     *
     * @return an Iterable of all the stores interned in this factory
     */
    Iterable<AwsStore> getAllStores();

    /**
     * Gets backend store for store name.
     *
     * @param store the name of the store to get
     * @return the KCVStore that corresponds to the store name provided
     */
    AwsStore getStore(String store);

}
