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

import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeySliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;

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

    public AbstractDynamoDBStore(final DynamoDBStoreManager manager, final String prefix, final String storeName) {
        this.manager = manager;
        this.client = this.manager.client();
        this.storeName = storeName;
        this.tableName = prefix + "_" + storeName;
        this.forceConsistentRead = client.forceConsistentRead();
    }

    /**
     * Creates the schemata for the DynamoDB table or tables each store requires.
     */
    public abstract CreateTableRequest getTableSchema();

    @Override
    public final void ensureStore() throws BackendException {
        LOG.info("Entering ensureStore name:{}", storeName);
        client.delegate().createTableAndWaitForActive(getTableSchema());
    }

    @Override
    public final void deleteStore() throws BackendException {
        LOG.info("Entering deleteStore name:{}", storeName);
        client.delegate().deleteTable(getTableSchema().getTableName());
        //block until the tables are actually deleted
        client.delegate().ensureTableDeleted(getTableSchema().getTableName());
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction txh) throws BackendException {
        DynamoDBStoreTransaction tx = DynamoDBStoreTransaction.getTx(txh);
        // Titan's locking expects that only the first expectedValue for a given key/column should be used
        if (!tx.contains(key, column)) {
            tx.put(key, column, expectedValue);
        }
    }

    @Override
    public void close() throws BackendException {
        LOG.info("Closing table:{}", tableName);
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
}
