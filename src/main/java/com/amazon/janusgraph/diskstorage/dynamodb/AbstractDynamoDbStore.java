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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.tuple.Pair;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.locking.TemporaryLockingException;

import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * The base class for the SINGLE and MULTI implementations of the Amazon DynamoDB Storage Backend
 * for JanusGraph distributed store type.
 * @author Matthew Sowders
 * @author Alexander Patrikalakis
 *
 */
@Slf4j
public abstract class AbstractDynamoDbStore implements AwsStore {
    protected final Client client;
    protected final String tableName;
    private final DynamoDBStoreManager manager;
    private final String storeName;
    private final boolean forceConsistentRead;
    private final Cache<Pair<StaticBuffer, StaticBuffer>, DynamoDbStoreTransaction> keyColumnLocalLocks;

    private static final class ReportingRemovalListener implements RemovalListener<Pair<StaticBuffer, StaticBuffer>, DynamoDbStoreTransaction> {
        private static final ReportingRemovalListener INSTANCE = new ReportingRemovalListener();
        private static ReportingRemovalListener theInstance() {
            return INSTANCE;
        }
        private ReportingRemovalListener() { }

        @Override
        public void onRemoval(final RemovalNotification<Pair<StaticBuffer, StaticBuffer>, DynamoDbStoreTransaction> notice) {
            log.trace("Expiring {} in tx {} because of {}", notice.getKey().toString(), notice.getValue().toString(), notice.getCause());
        }
    }

    protected CreateTableRequest createTableRequest() {
        return new CreateTableRequest()
            .withTableName(tableName)
            .withProvisionedThroughput(new ProvisionedThroughput(client.readCapacity(tableName),
                client.writeCapacity(tableName)));
    }

    protected void mutateOneKey(final StaticBuffer key, final KCVMutation mutation, final StoreTransaction txh) throws BackendException {
        manager.mutateMany(Collections.singletonMap(storeName, Collections.singletonMap(key, mutation)), txh);
    }

    @Override
    public String getName() {
        return storeName;
    }

    protected UpdateItemRequest createUpdateItemRequest() {
        return new UpdateItemRequest().withTableName(tableName);
    }

    protected GetItemRequest createGetItemRequest() {
        return new GetItemRequest().withTableName(tableName).withConsistentRead(forceConsistentRead);
    }

    protected QueryRequest createQueryRequest() {
        return new QueryRequest().withTableName(tableName).withConsistentRead(forceConsistentRead);
    }
    protected ScanRequest createScanRequest() {
        return new ScanRequest().withTableName(tableName).withConsistentRead(forceConsistentRead);
    }
    AbstractDynamoDbStore(final DynamoDBStoreManager manager, final String prefix, final String storeName) {
        this.manager = manager;
        this.client = this.manager.getClient();
        this.storeName = storeName;
        this.tableName = prefix + "_" + storeName;
        this.forceConsistentRead = client.isForceConsistentRead();

        final CacheBuilder<Pair<StaticBuffer, StaticBuffer>, DynamoDbStoreTransaction> builder = CacheBuilder.newBuilder().concurrencyLevel(client.getDelegate().getMaxConcurrentUsers())
            .expireAfterWrite(manager.getLockExpiresDuration().toMillis(), TimeUnit.MILLISECONDS)
            .removalListener(ReportingRemovalListener.theInstance());
        this.keyColumnLocalLocks = builder.build();
    }

    /**
     * Creates the schemata for the DynamoDB table or tables each store requires.
     * @return a create table request appropriate for the schema of the selected implementation.
     */
    public abstract CreateTableRequest getTableSchema();

    @Override
    public final void ensureStore() throws BackendException {
        log.debug("Entering ensureStore table:{}", tableName);
        client.getDelegate().createTableAndWaitForActive(getTableSchema());
    }

    @Override
    public final void deleteStore() throws BackendException {
        log.debug("Entering deleteStore name:{}", storeName);
        client.getDelegate().deleteTable(getTableSchema().getTableName());
        //block until the tables are actually deleted
        client.getDelegate().ensureTableDeleted(getTableSchema().getTableName());
    }

    @RequiredArgsConstructor
    private class SetStoreIfTxMappingDoesntExist implements Callable<DynamoDbStoreTransaction> {
        private final DynamoDbStoreTransaction tx;
        private final Pair<StaticBuffer, StaticBuffer> keyColumn;
        @Override
        public DynamoDbStoreTransaction call() throws Exception {
            tx.setStore(AbstractDynamoDbStore.this);
            log.trace(String.format("acquiring lock on %s at " + System.nanoTime(), keyColumn.toString()));
            // do not extend the expiry of an existing lock by the tx passed in this method
            return tx;
        }
    }

    @Override
    public void acquireLock(final StaticBuffer key, final StaticBuffer column, final StaticBuffer expectedValue, final StoreTransaction txh) throws BackendException {
        final DynamoDbStoreTransaction tx = DynamoDbStoreTransaction.getTx(txh);
        final Pair<StaticBuffer, StaticBuffer> keyColumn = Pair.of(key, column);

        final DynamoDbStoreTransaction existing;
        try {
            existing = keyColumnLocalLocks.get(keyColumn,
                new SetStoreIfTxMappingDoesntExist(tx, keyColumn));
        } catch (ExecutionException | UncheckedExecutionException | ExecutionError e) {
            throw new TemporaryLockingException("Unable to acquire lock", e);
        }
        if (null != existing && tx != existing) {
            throw new TemporaryLockingException(String.format("tx %s already locked key-column %s when tx %s tried to lock", existing.toString(), keyColumn.toString(), tx.toString()));
        }

        // Titan's locking expects that only the first expectedValue for a given key/column should be used
        if (!tx.contains(key, column)) {
            tx.put(key, column, expectedValue);
        }
    }

    @Override
    public void close() throws BackendException {
        log.debug("Closing table:{}", tableName);
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    protected String encodeKeyForLog(final StaticBuffer key) {
        if (null == key) {
            return "";
        }
        return Constants.HEX_PREFIX + Hex.encodeHexString(key.asByteBuffer().array());
    }

    String encodeForLog(final List<?> columns) {
        final StringBuilder result = new StringBuilder("[");
        for (int i = 0; i < columns.size(); i++) {
            final Object obj = columns.get(i);
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

    protected String encodeForLog(final SliceQuery query) {
        return "slice[rk:" + encodeKeyForLog(query.getSliceStart()) + " -> " + encodeKeyForLog(query.getSliceEnd()) + " limit:" + query.getLimit() + "]";
    }

    protected String encodeForLog(final KeySliceQuery query) {
        return "keyslice[hk:" + encodeKeyForLog(query.getKey()) + " " + "rk:" + encodeKeyForLog(query.getSliceStart()) + " -> " + encodeKeyForLog(query.getSliceEnd()) + " limit:"
            + query.getLimit() + "]";
    }

    void releaseLock(final StaticBuffer key, final StaticBuffer column) {
        keyColumnLocalLocks.invalidate(Pair.of(key, column));
    }
}
