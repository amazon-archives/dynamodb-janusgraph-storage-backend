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

import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.BaseTransactionConfig;
import com.thinkaurelius.titan.diskstorage.PermanentBackendException;
import com.amazon.titan.diskstorage.dynamodb.mutation.MutateWorker;
import com.google.common.collect.Lists;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.common.DistributedStoreManager;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KCVMutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyRange;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StandardStoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StandardStoreFeatures.Builder;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.util.time.Timestamps;

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

    public static final int getPort(final Configuration config) throws BackendException {
        final String endpoint = TitanConfigUtil.getNullableConfigValue(config, Constants.DYNAMODB_CLIENT_ENDPOINT);

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
                      .preferredTimestamps(Timestamps.MILLI) //ignored because timestamps is false
                      .storeTTL(false)
                      .timestamps(false)
                      .transactional(false)
                      .unorderedScan(true)
                      .visibility(false).build();
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {
        //this method can be called by titan-core, which is not aware of our backend implementation.
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

}
