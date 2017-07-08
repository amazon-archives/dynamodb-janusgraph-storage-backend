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

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.util.RecordIterator;

/**
 * KeyIterator that is backed by a DynamoDB scan. This class is ignorant to the fact that
 * its backing scan might be running in parallel. The ScanContextInterpreter is expected to
 * be compatible with whatever scan order the scanner is using.
 *
 * @author Michael Rodaitis
 */
public class ScanBackedKeyIterator implements KeyIterator {

    private final Scanner scanner;
    private final ScanContextInterpreter interpreter;

    private SingleKeyRecordIterator current;
    private Iterator<SingleKeyRecordIterator> recordIterators = Collections.emptyIterator();

    public ScanBackedKeyIterator(final Scanner scanner, final ScanContextInterpreter interpreter) {
        this.scanner = scanner;
        this.interpreter = interpreter;
    }

    @Override
    public RecordIterator<Entry> getEntries() {
        return current.getRecordIterator();
    }

    @Override
    public void close() throws IOException {
        scanner.close();
        recordIterators = Collections.emptyIterator();
    }

    @Override
    public boolean hasNext() {
        if (recordIterators.hasNext()) {
            return true;
        }

        while (scanner.hasNext() && !recordIterators.hasNext()) {
            nextScanResult();
        }
        return recordIterators.hasNext();
    }

    private void nextScanResult() {
        final ScanContext scanContext = scanner.next();
        final List<SingleKeyRecordIterator> newIterators = interpreter.buildRecordIterators(scanContext);
        recordIterators = newIterators.iterator();
    }

    @Override
    public StaticBuffer next() {
        current = recordIterators.next();
        return current.getKey();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove");
    }
}
