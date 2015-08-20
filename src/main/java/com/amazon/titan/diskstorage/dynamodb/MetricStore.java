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

import java.util.Collection;
import java.util.Map;

import com.amazon.titan.diskstorage.dynamodb.mutation.MutateWorker;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KCVMutation;
import com.thinkaurelius.titan.diskstorage.util.MetricInstrumentedStore;

/**
 * Proxies another store to centralize metrics gathering.
 *
 * @author Matthew Sowders
 *
 */
public class MetricStore extends MetricInstrumentedStore implements AwsStore {
    private final AwsStore delegate;

    public MetricStore(final AwsStore delegate) {
        super(delegate, delegate.getTableName());
        this.delegate = delegate;
    }

    @Override
    public void deleteStore() throws StorageException {
        delegate.deleteStore();
    }

    @Override
    public void ensureStore() throws StorageException {
        delegate.ensureStore();
    }

    @Override
    public String getTableName() {
        return delegate.getTableName();
    }

    @Override
    public Collection<MutateWorker> createMutationWorkers(Map<StaticBuffer, KCVMutation> mutationMap, DynamoDBStoreTransaction txh) {
        return delegate.createMutationWorkers(mutationMap, txh);
    }
}
