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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

import com.amazon.janusgraph.diskstorage.dynamodb.builder.EntryBuilder;
import com.amazon.janusgraph.diskstorage.dynamodb.builder.ItemBuilder;
import com.amazon.janusgraph.diskstorage.dynamodb.builder.SingleExpectedAttributeValueBuilder;
import com.amazon.janusgraph.diskstorage.dynamodb.builder.SingleUpdateBuilder;
import com.amazon.janusgraph.diskstorage.dynamodb.iterator.ScanBackedKeyIterator;
import com.amazon.janusgraph.diskstorage.dynamodb.iterator.ScanContextInterpreter;
import com.amazon.janusgraph.diskstorage.dynamodb.iterator.Scanner;
import com.amazon.janusgraph.diskstorage.dynamodb.iterator.SequentialScanner;
import com.amazon.janusgraph.diskstorage.dynamodb.iterator.SingleRowScanInterpreter;
import com.amazon.janusgraph.diskstorage.dynamodb.mutation.MutateWorker;
import com.amazon.janusgraph.diskstorage.dynamodb.mutation.SingleUpdateWithCleanupWorker;
import com.amazon.janusgraph.diskstorage.dynamodb.mutation.UpdateItemWorker;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.google.common.collect.Lists;

import lombok.extern.slf4j.Slf4j;

/**
 * Acts as if DynamoDB were a Column Oriented Database by using key as the hash
 * key and each entry has their own column. Note that if you are likely to go
 * over the DynamoDB 400kb per item limit you should use DynamoDbStore.
 *
 * See configuration
 * storage.dynamodb.stores.***store_name***.data-model=SINGLE
 *
 * KCV Schema - actual table (Hash(S) only):
 * hk   |  0x02  |  0x04    <-Attribute Names
 * 0x01 |  0x03  |  0x05    <-Row Values
 *
 * @author Matthew Sowders
 * @author Alexander Patrikalakis
 *
 */
@Slf4j
public class DynamoDbSingleRowStore extends AbstractDynamoDbStore {

    public DynamoDbSingleRowStore(final DynamoDBStoreManager manager, final String prefix, final String storeName) {
        super(manager, prefix, storeName);
    }

    @Override
    public CreateTableRequest getTableSchema() {
        return super.createTableRequest()
            .withAttributeDefinitions(
                new AttributeDefinition()
                    .withAttributeName(Constants.JANUSGRAPH_HASH_KEY)
                    .withAttributeType(ScalarAttributeType.S))
            .withKeySchema(
                new KeySchemaElement()
                    .withAttributeName(Constants.JANUSGRAPH_HASH_KEY)
                    .withKeyType(KeyType.HASH));
    }

    @Override
    public KeyIterator getKeys(final KeyRangeQuery query, final StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException("Keys are not byte ordered.");
    }

    private GetItemWorker createGetItemWorker(final StaticBuffer hashKey) {
        final GetItemRequest request = super.createGetItemRequest()
            .withKey(new ItemBuilder().hashKey(hashKey).build())
            .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        return new GetItemWorker(hashKey, request, client.getDelegate());
    }

    private EntryList extractEntriesFromGetItemResult(final GetItemResult result, final StaticBuffer sliceStart, final StaticBuffer sliceEnd, final int limit) {
        final Map<String, AttributeValue> item = result.getItem();
        List<Entry> filteredEntries = Collections.emptyList();
        if (null != item) {
            item.remove(Constants.JANUSGRAPH_HASH_KEY);
            filteredEntries = new EntryBuilder(item)
                    .slice(sliceStart, sliceEnd)
                    .limit(limit)
                    .buildAll();
        }
        return StaticArrayEntryList.of(filteredEntries);
    }

    @Override
    public KeyIterator getKeys(final SliceQuery query, final StoreTransaction txh) throws BackendException {
        log.debug("Entering getKeys table:{} query:{} txh:{}", getTableName(), encodeForLog(query), txh);

        final ScanRequest scanRequest = new ScanRequest().withTableName(tableName)
                                                         .withLimit(client.scanLimit(tableName))
                                                         .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);

        final Scanner scanner;
        if (client.isEnableParallelScan()) {
            scanner = client.getDelegate().getParallelScanCompletionService(scanRequest);
        } else {
            scanner = new SequentialScanner(client.getDelegate(), scanRequest);
        }
        // Because SINGLE records cannot be split across scan results, we can use the same interpreter for both
        // sequential and parallel scans.
        final ScanContextInterpreter interpreter = new SingleRowScanInterpreter(query);

        final KeyIterator result = new ScanBackedKeyIterator(scanner, interpreter);

        log.debug("Exiting getKeys table:{} query:{} txh:{} returning:{}", getTableName(), encodeForLog(query), txh, result);
        return result;
    }

    @Override
    public EntryList getSlice(final KeySliceQuery query, final StoreTransaction txh) throws BackendException {
        log.debug("Entering getSliceKeySliceQuery table:{} query:{} txh:{}", getTableName(), encodeForLog(query), txh);
        final GetItemRequest request = super.createGetItemRequest()
                .withKey(new ItemBuilder().hashKey(query.getKey()).build())
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        final GetItemResult result = new ExponentialBackoff.GetItem(request, client.getDelegate()).runWithBackoff();

        final List<Entry> filteredEntries = extractEntriesFromGetItemResult(result, query.getSliceStart(), query.getSliceEnd(), query.getLimit());
        log.debug("Exiting getSliceKeySliceQuery table:{} query:{} txh:{} returning:{}", getTableName(), encodeForLog(query), txh,
                  filteredEntries.size());
        return StaticArrayEntryList.of(filteredEntries);
    }

    @Override
    public Map<StaticBuffer, EntryList> getSlice(final List<StaticBuffer> keys, final SliceQuery query, final StoreTransaction txh) throws BackendException {
        log.debug("Entering getSliceMultiSliceQuery table:{} keys:{} query:{} txh:{}", getTableName(), encodeForLog(keys), encodeForLog(query),
                txh);
        final Map<StaticBuffer, EntryList> entries = new HashMap<>(keys.size());

        final List<GetItemWorker> getItemWorkers = Lists.newLinkedList();
        for (StaticBuffer hashKey : keys) {
            final GetItemWorker worker = createGetItemWorker(hashKey);
            getItemWorkers.add(worker);
        }

        final Map<StaticBuffer, GetItemResult> resultMap = client.getDelegate().parallelGetItem(getItemWorkers);
        for (Map.Entry<StaticBuffer, GetItemResult> resultEntry : resultMap.entrySet()) {
            final EntryList entryList = extractEntriesFromGetItemResult(resultEntry.getValue(), query.getSliceStart(),
                                                                  query.getSliceEnd(), query.getLimit());
            entries.put(resultEntry.getKey(), entryList);
        }

        log.debug("Exiting getSliceMultiSliceQuery table:{} keys:{} query:{} txh:{} returning:{}",
                getTableName(),
                encodeForLog(keys),
                encodeForLog(query),
                txh,
                entries.size());
        return entries;
    }

    @Override
    public void mutate(final StaticBuffer hashKey, final List<Entry> additions, final List<StaticBuffer> deletions, final StoreTransaction txh) throws BackendException {
        log.debug("Entering mutate table:{} keys:{} additions:{} deletions:{} txh:{}",
                  getTableName(),
                  encodeKeyForLog(hashKey),
                  encodeForLog(additions),
                  encodeForLog(deletions),
                  txh);
        super.mutateOneKey(hashKey, new KCVMutation(additions, deletions), txh);

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
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        final DynamoDbSingleRowStore rhs = (DynamoDbSingleRowStore) obj;
        return new EqualsBuilder().append(getTableName(), rhs.getTableName()).isEquals();
    }

    @Override
    public String toString() {
        return "DynamoDbSingleRowStore:" + getTableName();
    }

    @Override
    public Collection<MutateWorker> createMutationWorkers(final Map<StaticBuffer, KCVMutation> mutationMap, final DynamoDbStoreTransaction txh) {

        final List<MutateWorker> workers = Lists.newLinkedList();

        for (Map.Entry<StaticBuffer, KCVMutation> entry : mutationMap.entrySet()) {
            final StaticBuffer hashKey = entry.getKey();
            final KCVMutation mutation = entry.getValue();

            final Map<String, AttributeValue> key = new ItemBuilder().hashKey(hashKey)
                                                               .build();

            // Using ExpectedAttributeValue map to handle large mutations in a single request
            // Large mutations would require multiple requests using expressions
            final Map<String, ExpectedAttributeValue> expected = new SingleExpectedAttributeValueBuilder().key(hashKey)
                                                                                                    .transaction(txh)
                                                                                                    .build(mutation);

            final Map<String, AttributeValueUpdate> attributeValueUpdates =
                new SingleUpdateBuilder().deletions(mutation.getDeletions())
                    .additions(mutation.getAdditions())
                    .build();

            final UpdateItemRequest request = super.createUpdateItemRequest()
                   .withKey(key)
                   .withReturnValues(ReturnValue.ALL_NEW)
                   .withAttributeUpdates(attributeValueUpdates)
                   .withExpected(expected)
                   .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);

            final MutateWorker worker;
            if (mutation.hasDeletions() && !mutation.hasAdditions()) {
                worker = new SingleUpdateWithCleanupWorker(request, client.getDelegate());
            } else {
                worker = new UpdateItemWorker(request, client.getDelegate());
            }
            workers.add(worker);
        }
        return workers;
    }

}
