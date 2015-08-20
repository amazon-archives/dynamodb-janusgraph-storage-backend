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
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration.Restriction;
import com.thinkaurelius.titan.diskstorage.configuration.backend.CommonsConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

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
        Configuration config = createTestGraphConfig(backendDataModel);
        addElasticSearchConfig(config);
        TitanGraph graph = TitanFactory.open(config);
        return graph;
    }

    public static void tearDownGraph(TitanGraph graph) throws BackendException {
        if(null != graph) {
            graph.shutdown();
        }
        cleanUpTables();
    }

    public static Client createClient() {
        return new Client(new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS,
                                                 new CommonsConfiguration(loadProperties()),
                                                 Restriction.NONE));
    }

    public static Configuration loadProperties() {
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

        final Configuration search = config.subset("index").subset("search");
        search.setProperty("backend", "elasticsearch");
        search.setProperty("directory", tempSearchIndexDirectory.getAbsolutePath());
        final Configuration es = search.subset("elasticsearch");
        es.setProperty("client-only", "false");
        es.setProperty("local-mode", "true");
        return config;
    }

    public Configuration createTestGraphConfig(BackendDataModel backendDataModel) {

        String dataModelName = backendDataModel.name();

        final Configuration properties = loadProperties();
        final Configuration dynamodb = properties.subset("storage").subset("dynamodb");
        dynamodb.setProperty("prefix", dataModelName);
        dynamodb.setProperty("control-plane-rate", controlPlaneRate);

        final Configuration stores = dynamodb.subset("stores");
        final int tps = getTps();
        for (String store : Constants.REQUIRED_BACKEND_STORES) {
            final Configuration storeNs = stores.subset(store);
            configureStore(dataModelName, tps, storeNs, unlimitedIops);
        }

        return properties;
    }

    public int getTps() {
        return 750 * partitions;
    }

    public static void configureStore(String dataModelName, final int tps,
        final Configuration storeNs, boolean unlimitedIops) {
        storeNs.setProperty(Constants.STORES_DATA_MODEL.getName(), dataModelName);
        storeNs.setProperty(Constants.STORES_SCAN_LIMIT.getName(), 10000);
        storeNs.setProperty(Constants.STORES_CAPACITY_READ.getName(), tps);
        storeNs.setProperty(Constants.STORES_READ_RATE_LIMIT.getName(), unlimitedIops ? Integer.MAX_VALUE : tps);
        storeNs.setProperty(Constants.STORES_CAPACITY_WRITE.getName(), tps);
        storeNs.setProperty(Constants.STORES_WRITE_RATE_LIMIT.getName(), unlimitedIops ? Integer.MAX_VALUE : tps);
    }

    public String getUserLogName(String name) {
        return String.format("ulog_%s", name);
    }

    public CommonsConfiguration getWriteConfiguration(BackendDataModel model, List<String> extraStoreNames) {
        return getWriteConfiguration(model, extraStoreNames, 1 /*partitions*/);
    }

    public CommonsConfiguration getWriteConfiguration(BackendDataModel model, List<String> extraStoreNames, int partitions) {
        Preconditions.checkArgument(partitions > 0);
        Configuration config = TestGraphUtil.instance().createTestGraphConfig(model);
        Configuration cache = config.subset("cache");
        if(model == BackendDataModel.SINGLE) { //TODO refactor
            //default: 20000, testEdgesExceedCacheSize fails at 16792, passes at 16791
            //this is the maximum number of edges supported for a vertex with no vertex partitioning.
            cache.setProperty("tx-cache-size", 16791);
        }
        //necessary for simpleLogTest, simpleLogTestWithFailure
        for(String extraStoreName : extraStoreNames) {
            final int tps = TestGraphUtil.instance().getTps();
            final Configuration stores = config.subset("storage").subset("dynamodb").subset("stores");
            final Configuration ulog_test = stores.subset(extraStoreName);
            TestGraphUtil.configureStore(model.name(), tps, ulog_test, TestGraphUtil.instance().isUnlimitedIops());
        }
        if(partitions > 1) {
            final Configuration cluster = config.subset("cluster");
            cluster.addProperty("partition", "true");
            cluster.addProperty("max-partitions", Integer.toString(partitions));
        }
        CommonsConfiguration cc = new CommonsConfiguration(config);
        return cc;
    }

    public static void deleteAllTables(String prefix, DynamoDBDelegate delegate) throws BackendException {
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

    public static void cleanUpTables() throws BackendException {
        final Client client = TestGraphUtil.createClient();
        deleteAllTables(null /*prefix - delete all tables*/, client.delegate());
        client.delegate().shutdown();
    }
}
