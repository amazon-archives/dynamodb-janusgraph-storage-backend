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

import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.janusgraph.diskstorage.dynamodb.mutation.MutateWorker;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

/**
 * The Titan manager for the Amazon DynamoDB Storage Backend for Titan. Opens AwsStores. Tracks implemented
 * features. Implements mutateMany used by the concrete implementations.
 * @author Matthew Sowders
 * @author Alexander Patrikalakis
 *
 */
public class DynamoDBStoreManager extends DistributedStoreManager implements KeyColumnValueStoreManager {
    private static final Logger LOG = LoggerFactory.getLogger(DynamoDBStoreManager.class);
    @VisibleForTesting
    Client client;
    private final DynamoDBStoreFactory factory;
    private final StoreFeatures features;
    private final String prefix;
    private final String prefixAndMutateMany;
    private final String prefixAndMutateManyUpdateOrDeleteItemCalls;
    private final String prefixAndMutateManyKeys;
    private final String prefixAndMutateManyStores;
    private final Duration lockExpiryTime;

    public static final int getPort(final Configuration config) throws BackendException {
        final String endpoint = JanusGraphConfigUtil.getNullableConfigValue(config, Constants.DYNAMODB_CLIENT_ENDPOINT);

        int port = 8080;
        if (endpoint != null && !endpoint.equals(Constants.DYNAMODB_CLIENT_ENDPOINT.getDefaultValue())) {
            final URL url;
            try {
                url = new URL(endpoint);
            } catch(IOException e) {
                throw new PermanentBackendException("Unable to determine port from endpoint: " + endpoint);
            }
            port = url.getPort();
        }

        return port;
    }

    public DynamoDBStoreManager(Configuration backendConfig) throws BackendException {
        super(backendConfig, getPort(backendConfig));
        try {
            client = new Client(backendConfig);
        } catch(IllegalArgumentException e) {
            throw new PermanentBackendException("Bad configuration used: " + backendConfig.toString(), e);
        }
        prefix = client.getPrefix();
        factory = new TableNameDynamoDBStoreFactory();
        features = initializeFeatures(backendConfig);
        prefixAndMutateMany = String.format("%s_mutateMany", prefix);
        prefixAndMutateManyUpdateOrDeleteItemCalls = String.format("%s_mutateManyUpdateOrDeleteItemCalls", prefix);
        prefixAndMutateManyKeys = String.format("%s_mutateManyKeys", prefix);
        prefixAndMutateManyStores = String.format("%s_mutateManyStores", prefix);
        lockExpiryTime = backendConfig.get(GraphDatabaseConfiguration.LOCK_EXPIRE);
    }

    @Override
    public StoreTransaction beginTransaction(BaseTransactionConfig config) throws BackendException {
        DynamoDBStoreTransaction txh = new DynamoDBStoreTransaction(config);
        return txh;
    }

    @Override
    public void clearStorage() throws BackendException {
        LOG.debug("Entering clearStorage");
        for (AwsStore store : factory.getAllStores()) {
            store.deleteStore();
        }
        LOG.debug("Exiting clearStorage returning:void");
    }

    @Override
    public void close() throws BackendException {
        LOG.debug("Entering close");
        for (AwsStore store : factory.getAllStores()) {
            store.close();
        }
        client.delegate().shutdown();
        LOG.debug("Exiting close returning:void");
    }

    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public String getName() {
        LOG.debug("Entering getName");
        String name = getClass().getSimpleName() + prefix;
        LOG.debug("Exiting getName returning:{}", name);
        return name;
    }

    private StandardStoreFeatures initializeFeatures(Configuration config) {
        Builder builder = new StandardStoreFeatures.Builder();
        return builder.batchMutation(true)
                      .cellTTL(false)
                      .distributed(true)
                      .keyConsistent(config)
                      .keyOrdered(false)
                      .localKeyPartition(false)
                      .locking(true)
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
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {
        //this method can be called by janusgraph-core, which is not aware of our backend implementation.
        //that means the keys of mutations map are the logical store names.
        final Timer.Context ctxt = client.delegate().getTimerContext(this.prefixAndMutateMany, null /*tableName*/);
        try {
            final DynamoDBStoreTransaction tx = DynamoDBStoreTransaction.getTx(txh);

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

            client.delegate().getMeter(client.delegate().getMeterName(this.prefixAndMutateManyKeys, null /*tableName*/))
                .mark(keys);
            client.delegate().getMeter(client.delegate().getMeterName(this.prefixAndMutateManyUpdateOrDeleteItemCalls, null /*tableName*/))
                .mark(updateOrDeleteItemCalls);
            client.delegate().getMeter(client.delegate().getMeterName(this.prefixAndMutateManyStores, null /*tableName*/))
                .mark(mutations.size());
            client.delegate().parallelMutate(mutationWorkers);
        } finally {
            ctxt.stop();
        }
    }

    @Override
    public AwsStore openDatabase(String name) throws BackendException {
        return factory.create(this /*manager*/, prefix, name);
    }

    public Client client() {
        return client;
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Deployment getDeployment() {
        return client.delegate().isEmbedded() ? Deployment.EMBEDDED : Deployment.REMOTE;
    }

    @Override
    public KeyColumnValueStore openDatabase(String name, Container arg1) throws BackendException {
        // TODO revisit for TTL
        return factory.create(this /*manager*/, prefix, name);
    }

    public Duration getLockExpiresDuration() {
        return lockExpiryTime;
    }
}
