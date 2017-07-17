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


import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.amazon.janusgraph.testcategory.MultiIdAuthorityLogStoreCategory;
import com.amazon.janusgraph.testcategory.MultipleItemTestCategory;

/**
 *
 * @author Alexander Patrikalakis
 * @author Johan Jacobs
 *
*/
@Category({ MultiIdAuthorityLogStoreCategory.class, MultipleItemTestCategory.class })
public class MultiDynamoDBLogTest extends AbstractDynamoDBLogTest {
    public MultiDynamoDBLogTest() throws Exception {
        super(BackendDataModel.MULTI);
    }

    static private final long LONGER_TIMEOUT_MS = 120000;

    @Override
    @Test
    public void mediumSendReceiveSerial() throws Exception {
        simpleSendReceive(2000,50, LONGER_TIMEOUT_MS);
    }
    @Override
    @Test
    public void testMultipleReadersOnSingleLog() throws Exception {
        sendReceive(4, 2000, 50, false, LONGER_TIMEOUT_MS);
    }
    @Override
    @Test
    public void testMultipleReadersOnSingleLogSerial() throws Exception {
        sendReceive(4, 2000, 50, true, LONGER_TIMEOUT_MS);
    }
}
