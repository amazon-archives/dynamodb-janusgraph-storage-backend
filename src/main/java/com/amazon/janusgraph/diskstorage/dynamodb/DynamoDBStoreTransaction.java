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

import java.util.Map;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * Transaction is used to store expected values of each column for each key in a transaction
 *
 * @author Matthew Sowders
 * @author Michael Rodaitis
 * @author Alexander Patrikalakis
 *
 */
public class DynamoDBStoreTransaction extends AbstractStoreTransaction {

    private static final Logger LOG = LoggerFactory.getLogger(DynamoDBStoreTransaction.class);

    public static DynamoDBStoreTransaction getTx(StoreTransaction txh) {
        Preconditions.checkArgument(txh != null);
        Preconditions
                .checkArgument(txh instanceof DynamoDBStoreTransaction, "Unexpected transaction type %s", txh.getClass().getName());
        return (DynamoDBStoreTransaction) txh;
    }

    /**
     * This is only used for toString for debugging purposes.
     */
    private final String id;
    private final Map<StaticBuffer, Map<StaticBuffer, StaticBuffer>> expectedValues = Maps.newHashMap();
    private AbstractDynamoDBStore store;

    public DynamoDBStoreTransaction(BaseTransactionConfig config) {
        super(config);
        id = Constants.HEX_PREFIX + Long.toHexString(System.nanoTime());
        LOG.debug("begin id:{} config:{}", id, config);
    }

    public String getId() {
        return id;
    }

    @Override
    public void commit() throws BackendException {
        LOG.debug("commit id:{}", id);
        releaseLocks();
        expectedValues.clear();
        super.commit();
    }

    private void releaseLocks() {
        for(final Map.Entry<StaticBuffer, Map<StaticBuffer, StaticBuffer>> entry : expectedValues.entrySet()) {
            final StaticBuffer key = entry.getKey();
            for(final StaticBuffer column : entry.getValue().keySet()) {
                store.releaseLock(key, column);
            }
        }
    }

    public boolean contains(StaticBuffer key, StaticBuffer column) {
        if (expectedValues.containsKey(key)) {
            return expectedValues.get(key).containsKey(column);
        }
        return false;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof DynamoDBStoreTransaction)) {
            return false;
        }
        DynamoDBStoreTransaction rhs = (DynamoDBStoreTransaction) obj;
        return new EqualsBuilder()
                .append(id, rhs.id)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    public StaticBuffer get(StaticBuffer key, StaticBuffer column) {
        // This method assumes the caller has called contains(..) and received a positive response
        return expectedValues.get(key)
                             .get(column);
    }

    public void put(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue) {
        Map<StaticBuffer, StaticBuffer> valueMap;
        if (expectedValues.containsKey(key)) {
            valueMap = expectedValues.get(key);
        } else {
            valueMap = Maps.newHashMap();
            expectedValues.put(key, valueMap);
        }

        // Ignore any calls to put if we already have an expected value
        if (!valueMap.containsKey(column)) {
            valueMap.put(column, expectedValue);
        }
    }

    @Override
    public void rollback() throws BackendException {
        LOG.debug("rollback id:{}", id);
        releaseLocks();
        expectedValues.clear();
        super.rollback();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append(id).append(expectedValues).toString();
    }

    public void setStore(AbstractDynamoDBStore abstractDynamoDBStore) {
        this.store = abstractDynamoDBStore;
    }
}
