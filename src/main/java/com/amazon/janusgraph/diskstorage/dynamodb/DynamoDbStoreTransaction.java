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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Transaction is used to store expected values of each column for each key in a transaction
 *
 * @author Matthew Sowders
 * @author Michael Rodaitis
 * @author Alexander Patrikalakis
 *
 */
@Slf4j
public class DynamoDbStoreTransaction extends AbstractStoreTransaction { //CHECKSTYLE SUPPRESS - for the DB in DynamoDB

    public static DynamoDbStoreTransaction getTx(@NonNull final StoreTransaction txh) {
        Preconditions
                .checkArgument(txh instanceof DynamoDbStoreTransaction, "Unexpected transaction type %s", txh.getClass().getName());
        return (DynamoDbStoreTransaction) txh;
    }

    /**
     * This is only used for toString for debugging purposes.
     */
    private final String id;
    private final Map<StaticBuffer, Map<StaticBuffer, StaticBuffer>> expectedValues = Maps.newHashMap();
    private AbstractDynamoDbStore store;

    /**
     * Creates a DynamoDB Store transaction.
     * @param config the base transactional configuration.
     */
    public DynamoDbStoreTransaction(final BaseTransactionConfig config) {
        super(config);
        id = Constants.HEX_PREFIX + Long.toHexString(System.nanoTime());
        log.debug("begin id:{} config:{}", id, config);
    }

    @Override
    public void commit() throws BackendException {
        log.debug("commit id:{}", id);
        releaseLocks();
        expectedValues.clear();
        super.commit();
    }

    private void releaseLocks() {
        for (final Map.Entry<StaticBuffer, Map<StaticBuffer, StaticBuffer>> entry : expectedValues.entrySet()) {
            final StaticBuffer key = entry.getKey();
            for (final StaticBuffer column : entry.getValue().keySet()) {
                store.releaseLock(key, column);
            }
        }
    }

    /**
     * Determins whether a particular key and column are part of this transaction
     * @param key key to check for existence
     * @param column column to check for existence
     * @return true if both the key and column combination are in this transaction and false otherwise.
     */
    public boolean contains(final StaticBuffer key, final StaticBuffer column) {
        return expectedValues.containsKey(key) && expectedValues.get(key).containsKey(column);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof DynamoDbStoreTransaction)) {
            return false;
        }
        final DynamoDbStoreTransaction rhs = (DynamoDbStoreTransaction) obj;
        return new EqualsBuilder()
                .append(id, rhs.id)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    /**
     * Gets the expected value for a particular key and column, if any
     * @param key the key to get the expected value for
     * @param column the column to get the expected value for
     * @return the expected value of the given key-column pair, if any.
     */
    public StaticBuffer get(final StaticBuffer key, final StaticBuffer column) {
        // This method assumes the caller has called contains(..) and received a positive response
        return expectedValues.get(key)
                             .get(column);
    }

    /**
     * Puts the expected value for a particular key and column
     * @param key the key to put the expected value for
     * @param column the column to put the expected value for
     * @param expectedValue the expected value to put
     */
    public void put(final StaticBuffer key, final StaticBuffer column, final StaticBuffer expectedValue) {
        final Map<StaticBuffer, StaticBuffer> valueMap;
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
        log.debug("rollback id:{}", id);
        releaseLocks();
        expectedValues.clear();
        super.rollback();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append(id).append(expectedValues).toString();
    }

    public void setStore(final AbstractDynamoDbStore abstractDynamoDbStore) {
        this.store = abstractDynamoDbStore;
    }
}
