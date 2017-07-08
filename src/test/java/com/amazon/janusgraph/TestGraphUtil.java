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
package com.amazon.janusgraph;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.BasicConfiguration.Restriction;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;

import com.amazon.janusgraph.diskstorage.dynamodb.BackendDataModel;
import com.amazon.janusgraph.diskstorage.dynamodb.Client;
import com.amazon.janusgraph.diskstorage.dynamodb.Constants;
import com.amazon.janusgraph.diskstorage.dynamodb.DynamoDbDelegate;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 *
 * @author Alexander Patrikalakis
 * @author Michael Rodaitis
 *
 */
public enum TestGraphUtil {
    instance;

    private final int dynamoDBPartitions;
    private final int controlPlaneRate;
    private final boolean unlimitedIops;
    private final int provisionedReadAndWriteTps;
    private final File propertiesFile;

    TestGraphUtil() {
        dynamoDBPartitions = Integer.valueOf(System.getProperty("dynamodb-partitions", String.valueOf(1)));
        Preconditions.checkArgument(dynamoDBPartitions > 0);
        provisionedReadAndWriteTps = 750 * dynamoDBPartitions;
        unlimitedIops = Boolean.valueOf(System.getProperty("dynamodb-unlimited-iops", String.valueOf(Boolean.TRUE)));
        controlPlaneRate = Integer.valueOf(System.getProperty("dynamodb-control-plane-rate", String.valueOf(100)));
        Preconditions.checkArgument(controlPlaneRate > 0);
        //This is a configuration file for test code. not part of production applications.
        //no validation necessary.
        propertiesFile = new File(System.getProperty("properties-file", "src/test/resources/dynamodb-local.properties"));
        Preconditions.checkArgument(propertiesFile.exists());
        Preconditions.checkArgument(propertiesFile.isFile());
        Preconditions.checkArgument(propertiesFile.canRead());
    }

    public boolean isUnlimitedIops() {
        return unlimitedIops;
    }

    public JanusGraph openGraph(final BackendDataModel backendDataModel) {
        return JanusGraphFactory.open(createTestGraphConfig(backendDataModel));
    }

    public void tearDownGraph(final JanusGraph graph) throws BackendException {
        if(null != graph) {
            graph.close();
        }
        cleanUpTables();
    }

    @VisibleForTesting // used in ClientTest.java
    Client createClient() {
        return new Client(new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS,
                                                 new CommonsConfiguration(loadProperties()),
                                                 Restriction.NONE));
    }

    public Configuration loadProperties() {
        final PropertiesConfiguration storageConfig;
        try {
            storageConfig = new PropertiesConfiguration(propertiesFile);
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
        return storageConfig;
    }

    public Configuration createTestGraphConfig(final BackendDataModel backendDataModel) {
        final String dataModelName = backendDataModel.name();

        final Configuration properties = createTestConfig(backendDataModel);

        final String storesNsPrefix = "storage.dynamodb.stores.";
        final List<String> storeList = new ArrayList<>(Constants.REQUIRED_BACKEND_STORES);
        storeList.add(GraphDatabaseConfiguration.IDS_STORE_NAME.getDefaultValue());
        for (String store : storeList) {
            configureStore(dataModelName, provisionedReadAndWriteTps, properties, unlimitedIops, storesNsPrefix + store);
        }

        return properties;
    }
    
    private Configuration createTestConfig(final BackendDataModel backendDataModel) {
        final Configuration properties = loadProperties();
        final Configuration dynamodb = properties.subset("storage").subset("dynamodb");
        dynamodb.setProperty(Constants.DYNAMODB_TABLE_PREFIX.getName(), backendDataModel.name() /*prefix*/);
        dynamodb.setProperty(Constants.DYNAMODB_CONTROL_PLANE_RATE.getName(), controlPlaneRate);
        return properties;
    }

    private static void configureStore(final String dataModelName, final int tps,
        final Configuration config, final boolean unlimitedIops, final String prefix) {
        final String prefixPeriod = prefix + ".";
        config.setProperty(prefixPeriod + Constants.STORES_DATA_MODEL.getName(), dataModelName);
        config.setProperty(prefixPeriod + Constants.STORES_SCAN_LIMIT.getName(), 10000);
        config.setProperty(prefixPeriod + Constants.STORES_INITIAL_CAPACITY_READ.getName(), tps);
        config.setProperty(prefixPeriod + Constants.STORES_READ_RATE_LIMIT.getName(), unlimitedIops ? Integer.MAX_VALUE : tps);
        config.setProperty(prefixPeriod + Constants.STORES_INITIAL_CAPACITY_WRITE.getName(), tps);
        config.setProperty(prefixPeriod + Constants.STORES_WRITE_RATE_LIMIT.getName(), unlimitedIops ? Integer.MAX_VALUE : tps);
    }

    private static void configureStore(final String dataModelName, final int tps,
        final WriteConfiguration config, final boolean unlimitedIops, final String prefix) {
        final String prefixPeriod = prefix + ".";
        config.set(prefixPeriod + Constants.STORES_DATA_MODEL.getName(), dataModelName);
        config.set(prefixPeriod + Constants.STORES_SCAN_LIMIT.getName(), 10000);
        config.set(prefixPeriod + Constants.STORES_INITIAL_CAPACITY_READ.getName(), tps);
        config.set(prefixPeriod + Constants.STORES_READ_RATE_LIMIT.getName(), unlimitedIops ? Integer.MAX_VALUE : tps);
        config.set(prefixPeriod + Constants.STORES_INITIAL_CAPACITY_WRITE.getName(), tps);
        config.set(prefixPeriod + Constants.STORES_WRITE_RATE_LIMIT.getName(), unlimitedIops ? Integer.MAX_VALUE : tps);
    }

    public WriteConfiguration getStoreConfig(final BackendDataModel model,
            final List<String> storeNames) {
        return appendClusterPartitionsAndStores(model, new CommonsConfiguration(TestGraphUtil.instance.createTestConfig(model)), storeNames, 1 /*partitions*/);
    }

    public WriteConfiguration appendStoreConfig(final BackendDataModel model,
            final WriteConfiguration config, final List<String> storeNames) {
        final Configuration baseconfig = createTestConfig(model);
        final Iterator<String> it = baseconfig.getKeys();
        while(it.hasNext()) {
            final String key = it.next();
            config.set(key, baseconfig.getProperty(key));
        }
        return appendClusterPartitionsAndStores(model, config, storeNames, 1 /*titanClusterPartitions*/);
    }

    public WriteConfiguration graphConfigWithClusterPartitionsAndExtraStores(final BackendDataModel model,
            final List<String> extraStoreNames, final int janusGraphClusterPartitions) {
        return appendClusterPartitionsAndStores(model, new CommonsConfiguration(TestGraphUtil.instance.createTestGraphConfig(model)),
            extraStoreNames, janusGraphClusterPartitions);
    }

    public WriteConfiguration graphConfig(final BackendDataModel model) {
        return graphConfigWithClusterPartitionsAndExtraStores(model, Collections.emptyList(), 1);
    }

    public WriteConfiguration appendClusterPartitionsAndStores(final BackendDataModel model,
            final WriteConfiguration config, final List<String> storeNames,
            final int janusGraphClusterPartitions) {
        final Configuration baseconfig = createTestConfig(model);
        final Iterator<String> it = baseconfig.getKeys();
        while(it.hasNext()) {
            final String key = it.next();
            config.set(key, baseconfig.getProperty(key));
        }
        
        Preconditions.checkArgument(janusGraphClusterPartitions > 0);
        if(janusGraphClusterPartitions > 1) {
            config.set("cluster.max-partitions", Integer.toString(janusGraphClusterPartitions));
        }

        for(String store : storeNames) {
            final String prefix = "storage.dynamodb.stores." + store;
            TestGraphUtil.configureStore(model.name(), provisionedReadAndWriteTps, config, isUnlimitedIops(), prefix);
        }

        return config;
    }

    private static void deleteAllTables(final String prefix,
            final DynamoDbDelegate delegate) throws BackendException {
        final ListTablesResult result = delegate.listAllTables();
        for(String tableName : result.getTableNames()) {
            if(prefix != null && !tableName.startsWith(prefix)) {
                continue;
            }
            try {
                delegate.deleteTable(new DeleteTableRequest().withTableName(tableName));
            } catch (ResourceNotFoundException e) {
            }
        }
    }

    public void cleanUpTables() throws BackendException {
        final Client client = instance.createClient();
        deleteAllTables(null /*prefix - delete all tables*/, client.getDelegate());
        client.getDelegate().shutdown();
    }
}
