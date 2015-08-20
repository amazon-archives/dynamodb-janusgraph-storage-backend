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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;

/**
 * Creates backend store based on table name.
 *
 * @author Matthew Sowders
 * @author Alexander Patrikalakis
 *
 */
public class TableNameDynamoDBStoreFactory implements DynamoDBStoreFactory {
    private static final Logger LOG = LoggerFactory.getLogger(TableNameDynamoDBStoreFactory.class);
    private ConcurrentMap<String, AwsStore> stores = new ConcurrentHashMap<>();

    @Override
    public AwsStore create(DynamoDBStoreManager manager, String prefix, String name) throws StorageException {
        LOG.debug("Entering TableNameDynamoDBStoreFactory.create prefix:{} name:{}", prefix, name);
        // ensure there is only one instance used per table name.

        final Client client = manager.client();
        final BackendDataModel model = client.dataModel(name);
        if(model == null) {
            throw new PermanentStorageException(String.format("Store name %s unknown. Set up in properties", name));
        }
        final AwsStore storeBackend = model.createStoreBackend(manager, prefix, name);
        final AwsStore create = new MetricStore(storeBackend);
        final AwsStore previous = stores.putIfAbsent(name, create);
        if (null == previous) {
            try {
                create.ensureStore();
            } catch(StorageException e) {
                client.delegate().shutdown();
                throw e;
            }
        }
        final AwsStore store = stores.get(name);
        LOG.debug("Exiting TableNameDynamoDBStoreFactory.create prefix:{} name:{} returning:{}", prefix, name, store);
        return store;
    }

    @Override
    public Iterable<AwsStore> getAllStores() {
        return stores.values();
    }

}
