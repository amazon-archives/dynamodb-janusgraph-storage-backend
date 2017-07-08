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
import java.util.concurrent.ExecutionException;

import org.janusgraph.diskstorage.BackendException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.amazon.janusgraph.testcategory.IsolateSingleConcurrentGetSlice;
import com.amazon.janusgraph.testcategory.IsolateSingleConcurrentGetSliceAndMutate;
import com.amazon.janusgraph.testcategory.SingleDynamoDBStoreTestCategory;
import com.amazon.janusgraph.testcategory.SingleItemTestCategory;


/**
 *
 * @author Alexander Patrikalakis
 *
 */
public class SingleDynamoDBStoreTest extends AbstractDynamoDbStoreTest
{

    public SingleDynamoDBStoreTest()
    {
        super(BackendDataModel.SINGLE);
    }

    @Test
    @Override
    @Category({SingleDynamoDBStoreTestCategory.class, SingleItemTestCategory.class })
    public void testStoreTTL() throws Exception {
        super.testStoreTTL();
    }

    @Test
    @Override
    @Category({SingleDynamoDBStoreTestCategory.class, SingleItemTestCategory.class })
    public void storeAndRetrieveWithClosing() throws BackendException {
        super.storeAndRetrieveWithClosing();
    }

    @Test
    @Override
    @Category({SingleDynamoDBStoreTestCategory.class, SingleItemTestCategory.class })
    public void containsKeyColumnReturnsTrueOnExtantInput() throws Exception {
        super.containsKeyColumnReturnsTrueOnExtantInput();
    }

    @Test
    @Override
    @Category({SingleDynamoDBStoreTestCategory.class, SingleItemTestCategory.class })
    public void containsKeyColumnReturnsFalseOnNonexistentInput() throws Exception {
        super.containsKeyColumnReturnsFalseOnNonexistentInput();
    }

    @Test
    @Override
    @Category({SingleDynamoDBStoreTestCategory.class, SingleItemTestCategory.class })
    public void createDatabase() {
        super.createDatabase();
    }

    @Test
    @Override
    @Category({SingleDynamoDBStoreTestCategory.class, SingleItemTestCategory.class })
    public void intervalTest1() throws BackendException {
        super.intervalTest1();
    }

    @Test
    @Override
    @Category({SingleDynamoDBStoreTestCategory.class, SingleItemTestCategory.class })
    public void intervalTest2() throws BackendException {
        super.intervalTest2();
    }

    @Test
    @Override
    @Category({SingleDynamoDBStoreTestCategory.class, SingleItemTestCategory.class })
    public void scanTestWithSimpleJob() throws Exception {
        super.scanTestWithSimpleJob();
    }

    @Test
    @Override
    @Category({SingleDynamoDBStoreTestCategory.class, SingleItemTestCategory.class })
    public void testGetKeysColumnSlicesOnLowerTriangular() throws BackendException, IOException {
        super.testGetKeysColumnSlicesOnLowerTriangular();
    }

    @Test
    @Override
    @Category({SingleDynamoDBStoreTestCategory.class, SingleItemTestCategory.class })
    public void testGetKeysColumnSlicesSimple() throws BackendException {
        super.testGetKeysColumnSlicesSimple();
    }

    @Test
    @Override
    @Category({SingleDynamoDBStoreTestCategory.class, SingleItemTestCategory.class })
    public void getSliceRespectsAllBoundsInclusionArguments() throws Exception {
        super.getSliceRespectsAllBoundsInclusionArguments();
    }

    @Test
    @Override
    @Category({SingleDynamoDBStoreTestCategory.class, SingleItemTestCategory.class })
    public void scanTest() throws BackendException {
        super.scanTest();
    }

    @Test
    @Override
    @Category({SingleDynamoDBStoreTestCategory.class, SingleItemTestCategory.class })
    public void deleteColumnsTest1() throws BackendException {
        super.deleteColumnsTest1();
    }

    @Test
    @Override
    @Category({SingleDynamoDBStoreTestCategory.class, SingleItemTestCategory.class })
    public void deleteColumnsTest2() throws BackendException {
        super.deleteColumnsTest2();
    }

    @Test
    @Override
    @Category({SingleDynamoDBStoreTestCategory.class, SingleItemTestCategory.class })
    public void getNonExistentKeyReturnsNull() throws Exception {
        super.getNonExistentKeyReturnsNull();
    }

    @Test
    @Override
    @Category({SingleDynamoDBStoreTestCategory.class, SingleItemTestCategory.class })
    public void storeAndRetrievePerformance() throws BackendException {
        super.storeAndRetrievePerformance();
    }

    @Test
    @Override
    @Category({SingleDynamoDBStoreTestCategory.class, SingleItemTestCategory.class })
    public void storeAndRetrieve() throws BackendException {
        super.storeAndRetrieve();
    }

    @Test
    @Override
    @Category({SingleDynamoDBStoreTestCategory.class, SingleItemTestCategory.class })
    public void containsKeyReturnsTrueOnExtantKey() throws Exception {
        super.containsKeyReturnsTrueOnExtantKey();
    }

    @Test
    @Override
    @Category({SingleDynamoDBStoreTestCategory.class, SingleItemTestCategory.class })
    public void getSliceRespectsColumnLimit() throws Exception {
        super.getSliceRespectsColumnLimit();
    }

    @Test
    @Override
    @Category({SingleDynamoDBStoreTestCategory.class, SingleItemTestCategory.class })
    public void containsKeyReturnsFalseOnNonexistentKey() throws Exception {
        super.containsKeyReturnsFalseOnNonexistentKey();
    }

    @Test
    @Override
    @Category({SingleDynamoDBStoreTestCategory.class, SingleItemTestCategory.class })
    public void testGetKeysWithSliceQuery() throws Exception {
        super.testGetKeysWithSliceQuery();
    }

    @Test
    @Override
    @Category({SingleDynamoDBStoreTestCategory.class, SingleItemTestCategory.class })
    public void insertingGettingAndDeletingSimpleDataWorks() throws Exception {
        super.insertingGettingAndDeletingSimpleDataWorks();
    }

    @Test
    @Override
    @Category({IsolateSingleConcurrentGetSliceAndMutate.class, SingleItemTestCategory.class })
    public void testConcurrentGetSliceAndMutate() throws ExecutionException, InterruptedException, BackendException {
        super.testConcurrentGetSliceAndMutate();
    }

    @Test
    @Override
    @Category({SingleDynamoDBStoreTestCategory.class, SingleItemTestCategory.class })
    public void testGetSlices() throws Exception {
        super.testGetSlices();
    }

    @Test
    @Override
    @Category({SingleDynamoDBStoreTestCategory.class, SingleItemTestCategory.class })
    public void deleteKeys() throws BackendException {
        super.deleteKeys();
    }

    @Test
    @Override
    @Category({IsolateSingleConcurrentGetSlice.class, SingleItemTestCategory.class })
    public void testConcurrentGetSlice() throws ExecutionException, InterruptedException, BackendException {
        super.testConcurrentGetSlice();
    }

    @Test
    @Override
    @Category({SingleDynamoDBStoreTestCategory.class, SingleItemTestCategory.class })
    public void testOrderedGetKeysRespectsKeyLimit() throws BackendException {
        super.testOrderedGetKeysRespectsKeyLimit();
    }

    @Test
    @Override
    @Category({SingleDynamoDBStoreTestCategory.class, SingleItemTestCategory.class })
    public void testGetKeysWithKeyRange() throws Exception {
        super.testGetKeysWithKeyRange();
    }

    @Test
    @Override
    @Category({SingleDynamoDBStoreTestCategory.class, SingleItemTestCategory.class })
    public void testTtl() throws Exception {
        super.testTtl();
    }
}
