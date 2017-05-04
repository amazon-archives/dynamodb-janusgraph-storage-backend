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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRangeQuery;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.StaticArrayEntryList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.janusgraph.diskstorage.dynamodb.builder.ConditionExpressionBuilder;
import com.amazon.janusgraph.diskstorage.dynamodb.builder.EntryBuilder;
import com.amazon.janusgraph.diskstorage.dynamodb.builder.FilterExpressionBuilder;
import com.amazon.janusgraph.diskstorage.dynamodb.builder.ItemBuilder;
import com.amazon.janusgraph.diskstorage.dynamodb.builder.MultiUpdateExpressionBuilder;
import com.amazon.janusgraph.diskstorage.dynamodb.iterator.MultiRowParallelScanInterpreter;
import com.amazon.janusgraph.diskstorage.dynamodb.iterator.MultiRowSequentialScanInterpreter;
import com.amazon.janusgraph.diskstorage.dynamodb.iterator.ScanBackedKeyIterator;
import com.amazon.janusgraph.diskstorage.dynamodb.iterator.ScanContextInterpreter;
import com.amazon.janusgraph.diskstorage.dynamodb.iterator.Scanner;
import com.amazon.janusgraph.diskstorage.dynamodb.iterator.SequentialScanner;
import com.amazon.janusgraph.diskstorage.dynamodb.mutation.DeleteItemWorker;
import com.amazon.janusgraph.diskstorage.dynamodb.mutation.MutateWorker;
import com.amazon.janusgraph.diskstorage.dynamodb.mutation.UpdateItemWorker;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Acts as if DynamoDB were a Column Oriented Database by using range query when
 * required.
 *
 * See configuration
 * storage.dynamodb.stores.***table_name***.data-model=MULTI
 *
 * KCV Schema - actual table (Hash(S) + Range(S)):
 * hk(S)  |  rk(S)  |  v(B)  <-Attribute Names
 * 0x01   |  0x02   |  0x03  <-Row Values
 * 0x01   |  0x04   |  0x05  <-Row Values
 *
 * @author Matthew Sowders
 * @author Alexander Patrikalakis
 * @author Michael Rodaitis
 *
 */
public class DynamoDBStore extends AbstractDynamoDBStore {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    public DynamoDBStore(DynamoDBStoreManager manager, String prefix, String storeName) {
        super(manager, prefix, storeName);
    }

    private EntryList createEntryListFromItems(List<Map<String, AttributeValue>> items, SliceQuery sliceQuery) {
        List<Entry> entries = new ArrayList<>(items.size());
        for (Map<String, AttributeValue> item : items) {
            Entry entry = new EntryBuilder(item).slice(sliceQuery.getSliceStart(), sliceQuery.getSliceEnd())
                                                .build();
            if (null != entry) {
                entries.add(entry);
            }
        }
        return StaticArrayEntryList.of(entries);
    }

    public static final CreateTableRequest createTableRequest(String tableName, long rcu, long wcu) {
        CreateTableRequest req = new CreateTableRequest()
                .withAttributeDefinitions(
                        new AttributeDefinition()
                                .withAttributeName(Constants.JANUSGRAPH_HASH_KEY)
                                .withAttributeType(ScalarAttributeType.S),
                        new AttributeDefinition()
                                .withAttributeName(Constants.JANUSGRAPH_RANGE_KEY)
                                .withAttributeType(ScalarAttributeType.S))
                .withKeySchema(
                        new KeySchemaElement()
                                .withAttributeName(Constants.JANUSGRAPH_HASH_KEY)
                                .withKeyType(KeyType.HASH),
                        new KeySchemaElement()
                                .withAttributeName(Constants.JANUSGRAPH_RANGE_KEY)
                                .withKeyType(KeyType.RANGE))
                .withTableName(tableName)
                .withProvisionedThroughput(new ProvisionedThroughput()
                                                   .withReadCapacityUnits(rcu)
                                                   .withWriteCapacityUnits(wcu));
        return req;
    }

    @Override
    public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException("Byteorder is not maintained.");
    }

    @Override
    public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws BackendException {
        log.debug("Entering getKeys table:{} query:{} txh:{}", getTableName(), encodeForLog(query), txh);
        final Expression filterExpression = new FilterExpressionBuilder().rangeKey()
                                                                         .range(query)
                                                                         .build();

        final ScanRequest scanRequest = new ScanRequest().withTableName(tableName)
                                                         .withLimit(client.scanLimit(tableName))
                                                         .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                                                         .withFilterExpression(filterExpression.getConditionExpression())
                                                         .withExpressionAttributeValues(filterExpression.getAttributeValues());

        Scanner scanner;
        ScanContextInterpreter interpreter;
        if (client.enableParallelScan()) {
            scanner = client.delegate().getParallelScanCompletionService(scanRequest);
            interpreter = new MultiRowParallelScanInterpreter(this, query);
        } else {
            scanner = new SequentialScanner(client.delegate(), scanRequest);
            interpreter = new MultiRowSequentialScanInterpreter(this, query);
        }

        KeyIterator result = new ScanBackedKeyIterator(scanner, interpreter);
        log.debug("Exiting getKeys table:{} query:{} txh:{} returning:{}", getTableName(), encodeForLog(query), txh, result);
        return result;
    }

    private EntryList getKeysRangeQuery(StaticBuffer hashKey, SliceQuery query,
            StoreTransaction txh)
            throws BackendException {

        log.debug("Range query for hashKey:{} txh:{}", encodeKeyForLog(hashKey), txh);

        QueryWorker worker = buildQueryWorker(hashKey, query);
        QueryResultWrapper result = worker.call();

        return createEntryListFromItems(result.getDynamoDBResult().getItems(), query);
    }

    public QueryWorker buildQueryWorker(StaticBuffer hashKey, SliceQuery query) {
        final QueryRequest request = createQueryRequest(hashKey, query, forceConsistentRead, tableName);
        // Only enforce a limit when Titan tells us to
        if (query.hasLimit()) {
            int limit = query.getLimit();
            request.setLimit(limit);
            return new QueryWithLimitWorker(client.delegate(), request, hashKey, limit);
        }

        return new QueryWorker(client.delegate(), request, hashKey);
    }

    private QueryRequest createQueryRequest(StaticBuffer hashKey, SliceQuery rangeQuery, boolean consistentRead, String tableName) {
        Expression keyConditionExpression = new ConditionExpressionBuilder().hashKey(hashKey)
                                                                            .rangeKey(rangeQuery.getSliceStart(), rangeQuery.getSliceEnd())
                                                                            .build();

        final QueryRequest request = new QueryRequest().withConsistentRead(consistentRead)
                                                       .withTableName(tableName)
                                                       .withKeyConditionExpression(keyConditionExpression.getConditionExpression())
                                                       .withExpressionAttributeValues(keyConditionExpression.getAttributeValues())
                                                       .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        return request;
    }

    @Override
    public String getName() {
        return storeName;
    }

    @Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh)
            throws BackendException {
        log.debug("Entering getSliceKeySliceQuery table:{} query:{} txh:{}", getTableName(), encodeForLog(query), txh);
        EntryList result = getKeysRangeQuery(query.getKey(), query, txh);
        log.debug("Exiting getSliceKeySliceQuery table:{} query:{} txh:{} returning:{}", getTableName(), encodeForLog(query), txh,
                  result.size());
        return result;
    }

    @Override
    public Map<StaticBuffer, EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws BackendException {
        log.debug("Entering getSliceMultiSliceQuery table:{} keys:{} query:{} txh:{}",
                  getTableName(),
                  encodeForLog(keys),
                  encodeForLog(query),
                  txh);

        Map<StaticBuffer, EntryList> resultMap = Maps.newHashMapWithExpectedSize(keys.size());

        List<QueryWorker> queryWorkers = Lists.newLinkedList();
        for (StaticBuffer hashKey : keys) {
            QueryWorker queryWorker = buildQueryWorker(hashKey, query);
            queryWorkers.add(queryWorker);

            resultMap.put(hashKey, EntryList.EMPTY_LIST);
        }

        List<QueryResultWrapper> results = client.delegate().parallelQuery(queryWorkers);
        for (QueryResultWrapper resultWrapper : results) {
            StaticBuffer titanKey = resultWrapper.getTitanKey();

            QueryResult dynamoDBResult = resultWrapper.getDynamoDBResult();
            EntryList entryList = createEntryListFromItems(dynamoDBResult.getItems(), query);
            resultMap.put(titanKey, entryList);
        }

        log.debug("Exiting getSliceMultiSliceQuery table:{} keys:{} query:{} txh:{} returning:{}",
                  getTableName(),
                  encodeForLog(keys),
                  encodeForLog(query),
                  txh,
                  resultMap.size());
        return resultMap;
    }

    @Override
    public void mutate(StaticBuffer hashKey, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws BackendException {
        log.debug("Entering mutate table:{} keys:{} additions:{} deletions:{} txh:{}",
                  getTableName(),
                  encodeKeyForLog(hashKey),
                  encodeForLog(additions),
                  encodeForLog(deletions),
                  txh);
        KCVMutation mutation = new KCVMutation(additions, deletions);

        // this method also filters out deletions that are also added
        manager.mutateMany(Collections.singletonMap(storeName, Collections.singletonMap(hashKey, mutation)), txh);

        log.debug("Exiting mutate table:{} keys:{} additions:{} deletions:{} txh:{} returning:void",
                  getTableName(),
                  encodeKeyForLog(hashKey),
                  encodeForLog(additions),
                  encodeForLog(deletions),
                  txh);
    }

    @Override
    public int hashCode() {
        return getTableName().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        DynamoDBStore rhs = (DynamoDBStore) obj;
        return new EqualsBuilder().append(getTableName(), rhs.getTableName()).isEquals();
    }

    @Override
    public String toString() {
        return "DynamoDBKeyColumnValueStore:" + getTableName();
    }

    @Override
    public Collection<MutateWorker> createMutationWorkers(Map<StaticBuffer, KCVMutation> mutationMap, DynamoDBStoreTransaction txh) {
        List<MutateWorker> workers = new LinkedList<>();

        for (Map.Entry<StaticBuffer, KCVMutation> entry : mutationMap.entrySet()) {
            final StaticBuffer hashKey = entry.getKey();
            final KCVMutation mutation = entry.getValue();
            // Filter out deletions that are also added
            Set<StaticBuffer> add = new HashSet<>();

            for (Entry additionEntry : mutation.getAdditions()) {
                add.add(additionEntry.getColumn());
            }

            List<StaticBuffer> mutableDeletions = new LinkedList<StaticBuffer>(mutation.getDeletions());
            Iterator<StaticBuffer> iter = mutableDeletions.iterator();

            while (iter.hasNext()) {
                StaticBuffer deletionEntry = iter.next();
                if (add.contains(deletionEntry)) {
                    iter.remove();
                }
            }

            if (mutation.hasAdditions()) {
                workers.addAll(createWorkersForAdditions(hashKey, mutation.getAdditions(), tableName, txh));
            }
            if (!mutableDeletions.isEmpty()) {
                workers.addAll(createWorkersForDeletions(hashKey, mutableDeletions, tableName, txh));
            }
        }

        return workers;
    }

    private final Collection<MutateWorker> createWorkersForAdditions(StaticBuffer hashKey, List<Entry> additions, String tableName, DynamoDBStoreTransaction txh) {
        List<MutateWorker> workers = new LinkedList<>();
        for (Entry addition : additions) {
            final StaticBuffer rangeKey = addition.getColumn();
            Map<String, AttributeValue> keys = new ItemBuilder().hashKey(hashKey)
                                                                .rangeKey(rangeKey)
                                                                .build();

            final Expression updateExpression = new MultiUpdateExpressionBuilder().hashKey(hashKey)
                                                                                  .rangeKey(rangeKey)
                                                                                  .transaction(txh)
                                                                                  .value(addition.getValue())
                                                                                  .build();

            final UpdateItemRequest request = new UpdateItemRequest().withTableName(tableName)
                                                                     .withUpdateExpression(updateExpression.getUpdateExpression())
                                                                     .withConditionExpression(updateExpression.getConditionExpression())
                                                                     .withExpressionAttributeValues(updateExpression.getAttributeValues())
                                                                     .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                                                                     .withKey(keys);

            workers.add(new UpdateItemWorker(request, client.delegate()));
        }
        return workers;
    }

    private final Collection<MutateWorker> createWorkersForDeletions(StaticBuffer hashKey, List<StaticBuffer> deletions, String tableName, DynamoDBStoreTransaction txh) {
        List<MutateWorker> workers = new LinkedList<>();
        for (StaticBuffer rangeKey : deletions) {
            final Map<String, AttributeValue> keys = new ItemBuilder().hashKey(hashKey)
                                                                      .rangeKey(rangeKey)
                                                                      .build();

            final Expression updateExpression = new MultiUpdateExpressionBuilder().hashKey(hashKey)
                                                                                  .rangeKey(rangeKey)
                                                                                  .transaction(txh)
                                                                                  .build();

            final DeleteItemRequest request = new DeleteItemRequest().withTableName(tableName)
                                                                     .withConditionExpression(updateExpression.getConditionExpression())
                                                                     .withExpressionAttributeValues(updateExpression.getAttributeValues())
                                                                     .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                                                                     .withKey(keys);

            workers.add(new DeleteItemWorker(request, client.delegate()));
        }
        return workers;
    }

    @Override
    public CreateTableRequest getTableSchema() {
        return createTableRequest(tableName, client.readCapacity(tableName), client.writeCapacity(tableName));
    }

}
