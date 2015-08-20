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
package com.amazon.titan.diskstorage.dynamodb.test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.amazon.titan.diskstorage.dynamodb.BackendDataModel;
import com.amazon.titan.diskstorage.dynamodb.Client;
import com.amazon.titan.diskstorage.dynamodb.Constants;
import com.amazon.titan.diskstorage.dynamodb.DynamoDBDelegate;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.StorageException;

/**
 *
 * @author Alexander Patrikalakis
 * @author Michael Rodaitis
 *
 */
public class TestGraphUtil {
    private static final TestGraphUtil instance;
    static {
        instance = new TestGraphUtil();
    }
    public static TestGraphUtil instance() {
        return instance;
    }

    private final int partitions;
    private final int controlPlaneRate;
    private final boolean unlimitedIops;

    private TestGraphUtil() {
        partitions = Integer.valueOf(System.getProperty("dynamodb-partitions", String.valueOf(1)));
        Preconditions.checkArgument(partitions > 0);
        unlimitedIops = Boolean.valueOf(System.getProperty("dynamodb-unlimited-iops", String.valueOf(Boolean.TRUE)));
        controlPlaneRate = Integer.valueOf(System.getProperty("dynamodb-control-plane-rate", String.valueOf(1)));
        Preconditions.checkArgument(controlPlaneRate > 0);
    }

    public boolean isUnlimitedIops() {
        return unlimitedIops;
    }

    public TitanGraph openGraph(BackendDataModel backendDataModel) {
        Configuration config = createTestGraphConfig(backendDataModel);
        TitanGraph graph = TitanFactory.open(config);
        return graph;
    }

    public TitanGraph openGraphWithElasticSearch(BackendDataModel backendDataModel) {
        Configuration config = getElasticSearchConfiguration(backendDataModel);
        TitanGraph graph = TitanFactory.open(config);
        return graph;
    }

    public Configuration getElasticSearchConfiguration(BackendDataModel backendDataModel) {
        return addElasticSearchConfig(createTestGraphConfig(backendDataModel));
    }

    public static File getTempSearchIndexDirectory(Configuration config) {
        Configuration search = config.subset(Constants.STORAGE_NS).subset("index.search");
        return new File(search.getString("directory"));
    }

    public static void tearDownGraph(TitanGraph graph) throws StorageException {
        if(graph != null) {
            graph.shutdown();
        }
        cleanUpTables();
    }

    public static void deleteAllTables(String prefix, DynamoDBDelegate delegate) throws StorageException {
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

    public static void cleanUpTables() throws StorageException {
        final Client client = TestGraphUtil.createClient();
        deleteAllTables(null /*prefix*/, client.delegate());
        client.delegate().shutdown();
    }

    public static Client createClient() {
        return new Client(loadProperties().subset(Constants.STORAGE_NS));
    }

    public static PropertiesConfiguration loadProperties() {
        ClassLoader classLoader = Client.class.getClassLoader();
        URL resource = classLoader.getResource("META-INF/dynamodb_store_manager_test.properties");
        PropertiesConfiguration storageConfig;
        try {
            storageConfig = new PropertiesConfiguration(resource.getFile());
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
        return storageConfig;
    }

    public static Configuration addElasticSearchConfig(Configuration config) {
        File tempSearchIndexDirectory;
        try {
            tempSearchIndexDirectory = Files.createTempDirectory(null /*prefix*/).toFile();
        } catch (IOException e) {
            throw new IllegalStateException("unable to create search index temp dir", e);
        }

        //override search configuration
        Configuration search = config.subset("storage.index.search");
        search.setProperty("backend", "elasticsearch");
        search.setProperty("directory", tempSearchIndexDirectory.getAbsolutePath());
        search.setProperty("client-only", "false");
        search.setProperty("local-mode", "true");
        return config;
    }

    public Configuration createTestGraphConfig(BackendDataModel backendDataModel) {
        String dataModelName = backendDataModel.name();

        final Configuration properties = loadProperties();
        final Configuration dynamodb = properties.subset(Constants.STORAGE_NS).subset(Constants.DYNAMODB_NS);
        dynamodb.setProperty(Constants.TABLE_PREFIX, dataModelName);
        dynamodb.setProperty(Constants.TITAN_DYNAMODB_CONTROL_PLANE_RATE, controlPlaneRate);
        final Configuration stores = dynamodb.subset(Constants.STORES_NS);
        for (String storeName : Constants.BACKEND_STORE_NAMES) {
            final Configuration store = stores.subset(storeName);
            configureStore(dataModelName, getTps(), store, unlimitedIops);
        }

        return properties;
    }

    public Configuration addExtraStores(BackendDataModel model, Configuration config, List<String> extraStoreNames) {
        final Configuration stores = config.subset(Constants.STORAGE_NS).subset(Constants.DYNAMODB_NS).subset(Constants.STORES_NS);
        for(String extraStoreName : extraStoreNames) {
            final Configuration store = stores.subset(extraStoreName);
            configureStore(model.name(), getTps(), store, unlimitedIops);
        }
        return config;
    }

    public Configuration getConfiguration(BackendDataModel model, List<String> extraStores) {
        return addExtraStores(model, getElasticSearchConfiguration(model), extraStores);
    }

    public int getTps() {
        return 750 * partitions;
    }

    public static void configureStore(String dataModelName, final int tps,
        final Configuration storeNs, boolean unlimitedIops) {
        storeNs.setProperty(Constants.DATA_MODEL, dataModelName);
        storeNs.setProperty(Constants.SCAN_LIMIT, 10000);
        storeNs.setProperty(Constants.READ_CAPACITY, tps);
        storeNs.setProperty(Constants.READ_RATE_LIMIT, unlimitedIops ? Integer.MAX_VALUE : tps);
        storeNs.setProperty(Constants.WRITE_CAPACITY, tps);
        storeNs.setProperty(Constants.WRITE_RATE_LIMIT, unlimitedIops ? Integer.MAX_VALUE : tps);
    }
}
