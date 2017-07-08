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
package com.amazon.janusgraph.diskstorage.dynamodb.iterator;

import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.util.RecordIterator;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Holds a reference to a key and a RecordIterator for entries that correspond to that key.
 * Used as an intermediate value holder in AbstractLazyKeyIterator.
 *
 * @author Matthew Sowders
 * @author Alexander Patrikalakis
 * @author Michael Rodaitis
 */
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public class SingleKeyRecordIterator {

    @Getter
    private final StaticBuffer key;
    @Getter(AccessLevel.PACKAGE)
    private final RecordIterator<Entry> recordIterator;
}
