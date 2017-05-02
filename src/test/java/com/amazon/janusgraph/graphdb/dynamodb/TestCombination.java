/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazon.janusgraph.graphdb.dynamodb;

import static com.amazon.janusgraph.diskstorage.dynamodb.BackendDataModel.MULTI;
import static com.amazon.janusgraph.diskstorage.dynamodb.BackendDataModel.SINGLE;

import com.amazon.janusgraph.diskstorage.dynamodb.BackendDataModel;
import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Test combinations for Lock store tests
 *
 * @author Alexander Patrikalakis
 *
 */
@RequiredArgsConstructor
public enum TestCombination {
    SINGLE_ITEM_DYNAMODB_LOCKING(SINGLE, true),
    SINGLE_ITEM_JANUSGRAPH_LOCKING(SINGLE, false),
    MULTIPLE_ITEM_DYNAMODB_LOCKING(MULTI, true),
    MULTIPLE_ITEM_JANUSGRAPH_LOCKING(MULTI, false);

    @Getter
    private final BackendDataModel dataModel;
    @Getter
    private final Boolean useNativeLocking;

    public static final Collection<Object[]> LOCKING_CROSS_MODELS =
        Lists.newArrayList(
            SINGLE_ITEM_DYNAMODB_LOCKING, SINGLE_ITEM_JANUSGRAPH_LOCKING,
            MULTIPLE_ITEM_DYNAMODB_LOCKING, MULTIPLE_ITEM_JANUSGRAPH_LOCKING).stream()
            .map(Collections::singletonList)
            .map(List::toArray)
            .collect(Collectors.toList());

    public static final Collection<Object[]> NATIVE_LOCKING_CROSS_MODELS =
        Lists.newArrayList(
            SINGLE_ITEM_DYNAMODB_LOCKING,
            MULTIPLE_ITEM_DYNAMODB_LOCKING).stream()
            .map(Collections::singletonList)
            .map(List::toArray)
            .collect(Collectors.toList());



    public String toString() {
        return dataModel.getCamelCaseName() + (useNativeLocking ? "DynamoDB" : "JanusGraph") + "Locking";
    }
}