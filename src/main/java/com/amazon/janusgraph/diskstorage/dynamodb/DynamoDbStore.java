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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;

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
@Slf4j
public class DynamoDbStore extends AbstractDynamoDbStore {

    public DynamoDbStore(final DynamoDBStoreManager manager, final String prefix, final String storeName) {
        super(manager, prefix, storeName);
    }

    private EntryList createEntryListFromItems(final List<Map<String, AttributeValue>> items, final SliceQuery sliceQuery) {
        final List<Entry> entries = new ArrayList<>(items.size());
        for (Map<String, AttributeValue> item : items) {
            final Entry entry = new EntryBuilder(item).slice(sliceQuery.getSliceStart(), sliceQuery.getSliceEnd())
                                                .build();
            if (null != entry) {
                entries.add(entry);
            }
        }
        return StaticArrayEntryList.of(entries);
    }

    @Override
    public KeyIterator getKeys(final KeyRangeQuery query, final StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException("Byteorder is not maintained.");
    }

    @Override
    public KeyIterator getKeys(final SliceQuery query, final StoreTransaction txh) throws BackendException {
        log.debug("Entering getKeys table:{} query:{} txh:{}", getTableName(), encodeForLog(query), txh);
        final Expression filterExpression = new FilterExpressionBuilder().rangeKey()
                                                                         .range(query)
                                                                         .build();

        final ScanRequest scanRequest = super.createScanRequest()
                 .withFilterExpression(filterExpression.getConditionExpression())
                 .withExpressionAttributeValues(filterExpression.getAttributeValues());

        final Scanner scanner;
        final ScanContextInterpreter interpreter;
        if (client.isEnableParallelScan()) {
            scanner = client.getDelegate().getParallelScanCompletionService(scanRequest);
            interpreter = new MultiRowParallelScanInterpreter(this, query);
        } else {
            scanner = new SequentialScanner(client.getDelegate(), scanRequest);
            interpreter = new MultiRowSequentialScanInterpreter(this, query);
        }

        final KeyIterator result = new ScanBackedKeyIterator(scanner, interpreter);
        log.debug("Exiting getKeys table:{} query:{} txh:{} returning:{}", getTableName(), encodeForLog(query), txh, result);
        return result;
    }

    private EntryList getKeysRangeQuery(final StaticBuffer hashKey, final SliceQuery query,
            final StoreTransaction txh)
            throws BackendException {

        log.debug("Range query for hashKey:{} txh:{}", encodeKeyForLog(hashKey), txh);

        final QueryWorker worker = buildQueryWorker(hashKey, query);
        final QueryResultWrapper result = worker.call();

        return createEntryListFromItems(result.getDynamoDBResult().getItems(), query);
    }

    public QueryWorker buildQueryWorker(final StaticBuffer hashKey, final SliceQuery query) {
        final QueryRequest request = createQueryRequest(hashKey, query);
        // Only enforce a limit when Titan tells us to
        if (query.hasLimit()) {
            final int limit = query.getLimit();
            request.setLimit(limit);
            return new QueryWithLimitWorker(client.getDelegate(), request, hashKey, limit);
        }

        return new QueryWorker(client.getDelegate(), request, hashKey);
    }

    private QueryRequest createQueryRequest(final StaticBuffer hashKey, final SliceQuery rangeQuery) {
        final Expression keyConditionExpression = new ConditionExpressionBuilder().hashKey(hashKey)
                .rangeKey(rangeQuery.getSliceStart(), rangeQuery.getSliceEnd())
                .build();

        final QueryRequest request = super.createQueryRequest()
               .withKeyConditionExpression(keyConditionExpression.getConditionExpression())
               .withExpressionAttributeValues(keyConditionExpression.getAttributeValues());
        return request;
    }

    @Override
    public EntryList getSlice(final KeySliceQuery query, final StoreTransaction txh)
            throws BackendException {
        log.debug("Entering getSliceKeySliceQuery table:{} query:{} txh:{}", getTableName(), encodeForLog(query), txh);
        final EntryList result = getKeysRangeQuery(query.getKey(), query, txh);
        log.debug("Exiting getSliceKeySliceQuery table:{} query:{} txh:{} returning:{}", getTableName(), encodeForLog(query), txh,
                  result.size());
        return result;
    }

    @Override
    public Map<StaticBuffer, EntryList> getSlice(final List<StaticBuffer> keys, final SliceQuery query, final StoreTransaction txh) throws BackendException {
        log.debug("Entering getSliceMultiSliceQuery table:{} keys:{} query:{} txh:{}",
                  getTableName(),
                  encodeForLog(keys),
                  encodeForLog(query),
                  txh);

        final Map<StaticBuffer, EntryList> resultMap = Maps.newHashMapWithExpectedSize(keys.size());

        final List<QueryWorker> queryWorkers = Lists.newLinkedList();
        for (StaticBuffer hashKey : keys) {
            final QueryWorker queryWorker = buildQueryWorker(hashKey, query);
            queryWorkers.add(queryWorker);

            resultMap.put(hashKey, EntryList.EMPTY_LIST);
        }

        final List<QueryResultWrapper> results = client.getDelegate().parallelQuery(queryWorkers);
        for (QueryResultWrapper resultWrapper : results) {
            final StaticBuffer titanKey = resultWrapper.getTitanKey();

            final QueryResult dynamoDBResult = resultWrapper.getDynamoDBResult();
            final EntryList entryList = createEntryListFromItems(dynamoDBResult.getItems(), query);
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
    public void mutate(final StaticBuffer key, final List<Entry> additions, final List<StaticBuffer> deletions, final StoreTransaction txh) throws BackendException {
        log.debug("Entering mutate table:{} keys:{} additions:{} deletions:{} txh:{}",
                  getTableName(),
                  encodeKeyForLog(key),
                  encodeForLog(additions),
                  encodeForLog(deletions),
                  txh);
        // this method also filters out deletions that are also added
        super.mutateOneKey(key, new KCVMutation(additions, deletions), txh);

        log.debug("Exiting mutate table:{} keys:{} additions:{} deletions:{} txh:{} returning:void",
                  getTableName(),
                  encodeKeyForLog(key),
                  encodeForLog(additions),
                  encodeForLog(deletions),
                  txh);
    }

    @Override
    public String toString() {
        return "DynamoDBKeyColumnValueStore:" + getTableName();
    }

    @Override
    public Collection<MutateWorker> createMutationWorkers(final Map<StaticBuffer, KCVMutation> mutationMap, final DynamoDbStoreTransaction txh) {
        final List<MutateWorker> workers = new LinkedList<>();

        for (Map.Entry<StaticBuffer, KCVMutation> entry : mutationMap.entrySet()) {
            final StaticBuffer hashKey = entry.getKey();
            final KCVMutation mutation = entry.getValue();
            // Filter out deletions that are also added - TODO why use a set?
            final Set<StaticBuffer> add = mutation.getAdditions().stream()
                .map(Entry::getColumn).collect(Collectors.toSet());

            final List<StaticBuffer> mutableDeletions = mutation.getDeletions().stream()
                .filter(del -> !add.contains(del))
                .collect(Collectors.toList());

            if (mutation.hasAdditions()) {
                workers.addAll(createWorkersForAdditions(hashKey, mutation.getAdditions(), txh));
            }
            if (!mutableDeletions.isEmpty()) {
                workers.addAll(createWorkersForDeletions(hashKey, mutableDeletions, txh));
            }
        }

        return workers;
    }

    private Collection<MutateWorker> createWorkersForAdditions(final StaticBuffer hashKey, final List<Entry> additions, final DynamoDbStoreTransaction txh) {
        return additions.stream().map(addition -> {
                final StaticBuffer rangeKey = addition.getColumn();
                final Map<String, AttributeValue> keys = new ItemBuilder().hashKey(hashKey)
                    .rangeKey(rangeKey)
                    .build();

                final Expression updateExpression = new MultiUpdateExpressionBuilder().hashKey(hashKey)
                    .rangeKey(rangeKey)
                    .transaction(txh)
                    .value(addition.getValue())
                    .build();

                return super.createUpdateItemRequest()
                    .withUpdateExpression(updateExpression.getUpdateExpression())
                    .withConditionExpression(updateExpression.getConditionExpression())
                    .withExpressionAttributeValues(updateExpression.getAttributeValues())
                    .withKey(keys);
            })
            .map(request -> new UpdateItemWorker(request, client.getDelegate()))
            .collect(Collectors.toList());
    }

    private Collection<MutateWorker> createWorkersForDeletions(final StaticBuffer hashKey, final List<StaticBuffer> deletions, final DynamoDbStoreTransaction txh) {
        final List<MutateWorker> workers = new LinkedList<>();
        for (StaticBuffer rangeKey : deletions) {
            final Map<String, AttributeValue> keys = new ItemBuilder().hashKey(hashKey)
                                                                      .rangeKey(rangeKey)
                                                                      .build();

            final Expression updateExpression = new MultiUpdateExpressionBuilder().hashKey(hashKey)
                                                                                  .rangeKey(rangeKey)
                                                                                  .transaction(txh)
                                                                                  .build();

            final DeleteItemRequest request = super.createDeleteItemRequest().withKey(keys)
                     .withConditionExpression(updateExpression.getConditionExpression())
                     .withExpressionAttributeValues(updateExpression.getAttributeValues());


            workers.add(new DeleteItemWorker(request, client.getDelegate()));
        }
        return workers;
    }

    @Override
    public CreateTableRequest getTableSchema() {
        return super.getTableSchema()
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
                    .withKeyType(KeyType.RANGE));
    }

}
