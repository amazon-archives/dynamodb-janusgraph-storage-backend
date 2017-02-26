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

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeySliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.locking.TemporaryLockingException;

/**
 * The base class for the SINGLE and MULTI implementations of the Amazon DynamoDB Storage Backend
 * for Titan distributed store type.
 * @author Matthew Sowders
 * @author Alexander Patrikalakis
 *
 */
public abstract class AbstractDynamoDBStore implements AwsStore {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractDynamoDBStore.class);
    protected final DynamoDBStoreManager manager;
    protected final Client client;
    protected final String storeName;
    protected final String tableName;
    protected final boolean forceConsistentRead;
    private final Cache<Pair<StaticBuffer, StaticBuffer>, DynamoDBStoreTransaction> keyColumnLocalLocks;

    private static class ReportingRemovalListener implements RemovalListener<Pair<StaticBuffer, StaticBuffer>, DynamoDBStoreTransaction> {
        private static final ReportingRemovalListener INSTANCE = new ReportingRemovalListener();
        private static ReportingRemovalListener theInstance() {
            return INSTANCE;
        }
        private ReportingRemovalListener() {}

        @Override
        public void onRemoval(RemovalNotification<Pair<StaticBuffer, StaticBuffer>, DynamoDBStoreTransaction> notice) {
            LOG.trace("Expiring {} in tx {} because of {}", notice.getKey().toString(), notice.getValue().getId(), notice.getCause());
        }
    }

    public AbstractDynamoDBStore(final DynamoDBStoreManager manager, final String prefix, final String storeName) {
        this.manager = manager;
        this.client = this.manager.client();
        this.storeName = storeName;
        this.tableName = prefix + "_" + storeName;
        this.forceConsistentRead = client.forceConsistentRead();

        final CacheBuilder<Pair<StaticBuffer, StaticBuffer>, DynamoDBStoreTransaction> builder = CacheBuilder.newBuilder().concurrencyLevel(client.delegate().getMaxConcurrentUsers())
            .expireAfterWrite(manager.getLockExpiresDuration().toMillis(), TimeUnit.MILLISECONDS)
            .removalListener(ReportingRemovalListener.theInstance());
        this.keyColumnLocalLocks = builder.build();
    }

    /**
     * Creates the schemata for the DynamoDB table or tables each store requires.
     */
    public abstract CreateTableRequest getTableSchema();

    @Override
    public final void ensureStore() throws BackendException {
        LOG.debug("Entering ensureStore table:{}", tableName);
        client.delegate().createTableAndWaitForActive(getTableSchema());
    }

    @Override
    public final void deleteStore() throws BackendException {
        LOG.debug("Entering deleteStore name:{}", storeName);
        client.delegate().deleteTable(getTableSchema().getTableName());
        //block until the tables are actually deleted
        client.delegate().ensureTableDeleted(getTableSchema().getTableName());
    }

    private class SetStoreIfTxMappingDoesntExist implements Callable<DynamoDBStoreTransaction> {
        final DynamoDBStoreTransaction tx;
        final Pair<StaticBuffer, StaticBuffer> keyColumn;
        public SetStoreIfTxMappingDoesntExist(final DynamoDBStoreTransaction tx, final Pair<StaticBuffer, StaticBuffer> keyColumn) {
            this.tx = tx;
            this.keyColumn = keyColumn;
        }
        @Override
        public DynamoDBStoreTransaction call() throws Exception {
            tx.setStore(AbstractDynamoDBStore.this);
            LOG.trace(String.format("acquiring lock on %s at " + System.nanoTime(), keyColumn.toString()));
            // do not extend the expiry of an existing lock by the tx passed in this method
            return tx;
        }
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction txh) throws BackendException {
        final DynamoDBStoreTransaction tx = DynamoDBStoreTransaction.getTx(txh);
        final Pair<StaticBuffer, StaticBuffer> keyColumn = Pair.of(key, column);

        final DynamoDBStoreTransaction existing;
        try {
            existing = keyColumnLocalLocks.get(keyColumn,
                new SetStoreIfTxMappingDoesntExist(tx, keyColumn));
        } catch (ExecutionException | UncheckedExecutionException | ExecutionError e) {
            throw new TemporaryLockingException("Unable to acquire lock", e);
        }
        if(null != existing && tx != existing) {
            throw new TemporaryLockingException(String.format("tx %s already locked key-column %s when tx %s tried to lock", existing.getId(), keyColumn.toString(), tx.getId()));
        }

        // Titan's locking expects that only the first expectedValue for a given key/column should be used
        if (!tx.contains(key, column)) {
            tx.put(key, column, expectedValue);
        }
    }

    @Override
    public void close() throws BackendException {
        LOG.debug("Closing table:{}", tableName);
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    protected String encodeKeyForLog(StaticBuffer key) {
        if (null == key) {
            return "";
        }
        return Constants.HEX_PREFIX + Hex.encodeHexString(key.asByteBuffer().array());
    }

    protected String encodeKeyForLog(StaticBuffer hashKey, StaticBuffer columnKey) {
        return encodeKeyForLog(hashKey) + ":" + encodeKeyForLog(columnKey);
    }

    protected String encodeForLog(List<?> columns) {
        StringBuilder result = new StringBuilder("[");
        for (int i = 0; i < columns.size(); i++) {
            Object obj = columns.get(i);
            StaticBuffer column = null;
            if (obj instanceof StaticBuffer) {
                column = (StaticBuffer) obj;
            } else if (obj instanceof Entry) {
                column = ((Entry) obj).getColumn();
            }
            result.append(encodeKeyForLog(column));
            if (i < columns.size() - 1) {
                result.append(",");
            }
        }
        return result.append("]").toString();
    }

    protected String encodeForLog(SliceQuery query) {
        return new StringBuilder("slice[rk:")
                .append(encodeKeyForLog(query.getSliceStart()))
                .append(" -> ")
                .append(encodeKeyForLog(query.getSliceEnd()))
                .append(" limit:")
                .append(query.getLimit())
                .append("]")
                .toString();
    }

    protected String encodeForLog(KeySliceQuery query) {
        return new StringBuilder("keyslice[hk:")
                .append(encodeKeyForLog(query.getKey()))
                .append(" ")
                .append("rk:")
                .append(encodeKeyForLog(query.getSliceStart()))
                .append(" -> ")
                .append(encodeKeyForLog(query.getSliceEnd()))
                .append(" limit:")
                .append(query.getLimit())
                .append("]")
                .toString();
    }

    public void releaseLock(StaticBuffer key, StaticBuffer column) {
        keyColumnLocalLocks.invalidate(Pair.of(key, column));
    }
}
