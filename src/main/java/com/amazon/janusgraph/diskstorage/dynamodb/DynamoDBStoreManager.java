/*
 * Copyright 2014-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.StoreMetaData.Container;
import org.janusgraph.diskstorage.common.DistributedStoreManager;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StandardStoreFeatures.Builder;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;

import com.amazon.janusgraph.diskstorage.dynamodb.mutation.MutateWorker;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * The JanusGraph manager for the Amazon DynamoDB Storage Backend for JanusGraph. Opens AwsStores. Tracks implemented
 * features. Implements mutateMany used by the concrete implementations.
 * @author Matthew Sowders
 * @author Alexander Patrikalakis
 *
 */
@Slf4j
public class DynamoDBStoreManager extends DistributedStoreManager implements KeyColumnValueStoreManager {
    private static final int DEFAULT_PORT = 8080;
    @VisibleForTesting
    @Getter
    Client client;
    private final DynamoDbStoreFactory factory;
    @Getter
    private final StoreFeatures features;
    private final String prefix;
    private final String prefixAndMutateMany;
    private final String prefixAndMutateManyUpdateOrDeleteItemCalls;
    private final String prefixAndMutateManyKeys;
    private final String prefixAndMutateManyStores;
    private final Duration lockExpiryTime;

    private static int getPort(final Configuration config) throws BackendException {
        final String endpoint = JanusGraphConfigUtil.getNullableConfigValue(config, Constants.DYNAMODB_CLIENT_ENDPOINT);

        int port = DEFAULT_PORT;
        if (endpoint != null && !endpoint.equals(Constants.DYNAMODB_CLIENT_ENDPOINT.getDefaultValue())) {
            final URL url;
            try {
                url = new URL(endpoint);
            } catch (IOException e) {
                throw new PermanentBackendException("Unable to determine port from endpoint: " + endpoint);
            }
            port = url.getPort();
        }

        return port;
    }

    public DynamoDBStoreManager(final Configuration backendConfig) throws BackendException {
        super(backendConfig, getPort(backendConfig));
        try {
            client = new Client(backendConfig);
        } catch (IllegalArgumentException e) {
            throw new PermanentBackendException("Bad configuration used: " + backendConfig.toString(), e);
        }
        prefix = client.getPrefix();
        factory = new TableNameDynamoDbStoreFactory();
        features = initializeFeatures(backendConfig);
        prefixAndMutateMany = String.format("%s_mutateMany", prefix);
        prefixAndMutateManyUpdateOrDeleteItemCalls = String.format("%s_mutateManyUpdateOrDeleteItemCalls", prefix);
        prefixAndMutateManyKeys = String.format("%s_mutateManyKeys", prefix);
        prefixAndMutateManyStores = String.format("%s_mutateManyStores", prefix);
        lockExpiryTime = backendConfig.get(GraphDatabaseConfiguration.LOCK_EXPIRE);
    }

    @Override
    public StoreTransaction beginTransaction(final BaseTransactionConfig config) throws BackendException {
        final DynamoDbStoreTransaction txh = new DynamoDbStoreTransaction(config);
        return txh;
    }

    @Override
    public void clearStorage() throws BackendException {
        log.debug("Entering clearStorage");
        for (AwsStore store : factory.getAllStores()) {
            store.deleteStore();
        }
        log.debug("Exiting clearStorage returning:void");
    }

    @Override
    public boolean exists() throws BackendException {
        return client.getDelegate().listTables(new ListTablesRequest()) != null;
    }

    @Override
    public void close() throws BackendException {
        log.debug("Entering close");
        for (AwsStore store : factory.getAllStores()) {
            store.close();
        }
        client.getDelegate().shutdown();
        log.debug("Exiting close returning:void");
    }

    @Override
    public String getName() {
        log.debug("Entering getName");
        final String name = getClass().getSimpleName() + prefix;
        log.debug("Exiting getName returning:{}", name);
        return name;
    }

    private StandardStoreFeatures initializeFeatures(final Configuration config) {
        final Builder builder = new StandardStoreFeatures.Builder();
        return builder.batchMutation(true)
                      .cellTTL(false)
                      .distributed(true)
                      .keyConsistent(config)
                      .keyOrdered(false)
                      .localKeyPartition(false)
                      .locking(config.get(Constants.DYNAMODB_USE_NATIVE_LOCKING))
                      .multiQuery(true)
                      .orderedScan(false)
                      .preferredTimestamps(TimestampProviders.MILLI) //ignored because timestamps is false
                      .storeTTL(false)
                      .timestamps(false)
                      .transactional(false)
                      .supportsInterruption(false)
                      .optimisticLocking(true)
                      .unorderedScan(true)
                      .visibility(false).build();
    }

    @Override
    public void mutateMany(final Map<String, Map<StaticBuffer, KCVMutation>> mutations, final StoreTransaction txh) throws BackendException {
        //this method can be called by janusgraph-core, which is not aware of our backend implementation.
        //that means the keys of mutations map are the logical store names.
        final Timer.Context ctxt = client.getDelegate().getTimerContext(this.prefixAndMutateMany, null /*tableName*/);
        try {
            final DynamoDbStoreTransaction tx = DynamoDbStoreTransaction.getTx(txh);

            final List<MutateWorker> mutationWorkers = Lists.newLinkedList();

            // one pass to create tasks
            long updateOrDeleteItemCalls = 0;
            long keys = 0;
            for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> mutationMapEntry : mutations.entrySet()) {
                final AwsStore store = openDatabase(mutationMapEntry.getKey());

                final Map<StaticBuffer, KCVMutation> storeMutations = mutationMapEntry.getValue();
                keys += storeMutations.size();
                final Collection<MutateWorker> storeWorkers = store.createMutationWorkers(storeMutations, tx);
                updateOrDeleteItemCalls += storeWorkers.size();

                mutationWorkers.addAll(storeWorkers);
            }

            // shuffle the list of MutationWorkers so writes to edgestore and graphindex happen in parallel
            Collections.shuffle(mutationWorkers);

            client.getDelegate().getMeter(client.getDelegate().getMeterName(this.prefixAndMutateManyKeys, null /*tableName*/))
                .mark(keys);
            client.getDelegate().getMeter(client.getDelegate().getMeterName(this.prefixAndMutateManyUpdateOrDeleteItemCalls, null /*tableName*/))
                .mark(updateOrDeleteItemCalls);
            client.getDelegate().getMeter(client.getDelegate().getMeterName(this.prefixAndMutateManyStores, null /*tableName*/))
                .mark(mutations.size());
            client.getDelegate().parallelMutate(mutationWorkers);
        } finally {
            ctxt.stop();
        }
    }

    @Override
    public AwsStore openDatabase(@NonNull final String name) throws BackendException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name), "database name may not be null or empty");
        return factory.create(this /*manager*/, prefix, name);
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Deployment getDeployment() {
        if (client.getDelegate().isEmbedded()) {
            return Deployment.EMBEDDED;
        } else {
            return Deployment.REMOTE;
        }
    }

    @Override
    public KeyColumnValueStore openDatabase(final String name, final Container arg1) throws BackendException {
        // TODO revisit for TTL
        // https://github.com/awslabs/dynamodb-titan-storage-backend/issues/70
        return factory.create(this /*manager*/, prefix, name);
    }

    public Duration getLockExpiresDuration() {
        return lockExpiryTime;
    }
}
