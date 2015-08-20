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

import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.titan.diskstorage.dynamodb.mutation.MutateWorker;
import com.google.common.annotations.VisibleForTesting;
import com.codahale.metrics.Timer;
import com.google.common.collect.Lists;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.common.DistributedStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KCVMutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTxConfig;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

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
    final Client client;
    private final DynamoDBStoreFactory factory;
    private final StoreFeatures features;
    private final String prefix;
    private final String prefixAndMutateMany;
    private final String prefixAndMutateManyUpdateOrDeleteItemCalls;
    private final String prefixAndMutateManyKeys;
    private final String prefixAndMutateManyStores;

    public static final Configuration decorateMachineIdIfNoEndpoint(final Configuration backendConfig) {
        final String endpoint = backendConfig.subset(Constants.DYNAMODB_NS).subset(Constants.CLIENT_NS).getString(Constants.CLIENT_ENDPOINT);
        if (endpoint == null) {
            //DynamoDBLocal embedded, so force the machine ID as DNS may not be working
            backendConfig.setProperty(GraphDatabaseConfiguration.INSTANCE_RID_RAW_KEY, "DEADBEEF");
        }
        return backendConfig;
    }

    public static final int getPort(final Configuration backendConfig) throws StorageException {
        final String endpoint = backendConfig.subset(Constants.DYNAMODB_NS).subset(Constants.CLIENT_NS).getString(Constants.CLIENT_ENDPOINT);

        int port = 8080;
        if (endpoint != null) {
            final URL url;
            try {
                url = new URL(endpoint);
            } catch(IOException e) {
                throw new PermanentStorageException("Unable to determine port from endpoint: " + endpoint);
            }
            port = url.getPort();
        }

        return port;
    }

    public DynamoDBStoreManager(Configuration backendConfig) throws StorageException {
        super(backendConfig, getPort(decorateMachineIdIfNoEndpoint(backendConfig)));
        try {
            client = new Client(backendConfig);
        } catch(IllegalArgumentException e) {
            throw new PermanentStorageException("Bad configuration used: " + backendConfig.toString(), e);
        }
        prefix = client.getPrefix();
        factory = new TableNameDynamoDBStoreFactory();

        features = initializeFeatures();
        prefixAndMutateMany = String.format("%s_mutateMany", prefix);
        prefixAndMutateManyUpdateOrDeleteItemCalls = String.format("%s_mutateManyUpdateOrDeleteItemCalls", prefix);
        prefixAndMutateManyKeys = String.format("%s_mutateManyKeys", prefix);
        prefixAndMutateManyStores = String.format("%s_mutateManyStores", prefix);
    }

    @Override
    public StoreTransaction beginTransaction(StoreTxConfig config) throws StorageException {
        DynamoDBStoreTransaction txh = new DynamoDBStoreTransaction(config, rid);
        return txh;
    }

    @Override
    public void clearStorage() throws StorageException {
        LOG.debug("Entering clearStorage");
        for (AwsStore store : factory.getAllStores()) {
            store.deleteStore();
        }
        LOG.debug("Exiting clearStorage returning:void");
    }

    @Override
    public void close() throws StorageException {
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

    private static StoreFeatures initializeFeatures() {
        final StoreFeatures features = new StoreFeatures();

        features.hasLocalKeyPartition = false;
        features.isDistributed = true;
        features.isKeyOrdered = false;
        features.supportsBatchMutation = true;
        features.supportsConsistentKeyOperations = true;
        features.supportsLocking = true;
        features.supportsMultiQuery = true;
        features.supportsOrderedScan = false;
        features.supportsTransactions = false;
        features.supportsUnorderedScan = true;

        return features;
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws StorageException {
        //this method can be called by titan-core, which is not aware of our backend implementation.
        //that means the keys of mutations map are the logical store names.
        final Timer.Context ctxt = client.delegate().getTimerContext(this.prefixAndMutateMany , null /*tableName*/);
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
    public AwsStore openDatabase(String name) throws StorageException {
        return factory.create(this /*manager*/, prefix, name);
    }

    public Client client() {
        return client;
    }

    public Deployment getDeployment() {
        return client.delegate().isEmbedded() ? Deployment.EMBEDDED : Deployment.REMOTE;
    }
}
