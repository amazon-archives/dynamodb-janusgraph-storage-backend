/*
 * Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Portions copyright Titan: Distributed Graph Database - Copyright 2012 and onwards Aurelius.
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
package com.amazon.titan.graphdb.dynamodb;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.titan.diskstorage.dynamodb.BackendDataModel;
import com.amazon.titan.diskstorage.dynamodb.test.TestGraphUtil;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;

public abstract class AbstractDynamoDBGraphTest extends TitanGraphTest
{
    private File tempSearchIndexDirectory;
    protected final Logger log;
    protected final String dataModelName;
    @Rule
    public TestName name = new TestName();
    public AbstractDynamoDBGraphTest(Class<?> clazz, BackendDataModel dataModel)
    {
        super(TestGraphUtil.instance().getElasticSearchConfiguration(dataModel));
        this.tempSearchIndexDirectory = TestGraphUtil.getTempSearchIndexDirectory(super.config);
        this.log = LoggerFactory.getLogger(clazz);
        this.dataModelName = dataModel.name();
    }

    @After
    public void after() throws IOException {
        FileUtils.cleanDirectory(tempSearchIndexDirectory);
        tempSearchIndexDirectory.delete();
    }

    @AfterClass
    public static void deleteTables() throws StorageException {
        TestGraphUtil.cleanUpTables();
    }
}
