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
import java.util.BitSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;

import org.janusgraph.diskstorage.BackendException;

import com.amazon.janusgraph.diskstorage.dynamodb.BackendRuntimeException;
import com.amazon.janusgraph.diskstorage.dynamodb.DynamoDBDelegate;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;

/**
 * Class lazily loads all the pages of all the segments of a parallel scan, in multiple threads.
 * @author Alexander Patrikalakis
 *
 */
public class ParallelScanner implements Scanner {
    private final BitSet finished;
    private final ScanSegmentWorker[] workers;
    private final ExecutorCompletionService<ScanContext> exec;
    private final DynamoDBDelegate dynamoDBDelegate;

    // contains all currently running ScanRequests
    private final Future<ScanContext>[] currentFutures;

    @SuppressWarnings("unchecked") //TODO(alexp) turn this into some Collections
    public ParallelScanner(Executor executor, int segments, DynamoDBDelegate dynamoDBDelegate) {
        this.dynamoDBDelegate = dynamoDBDelegate;
        this.exec = new ExecutorCompletionService<>(executor);
        this.finished = new BitSet(segments);
        this.finished.clear();
        this.workers = new ScanSegmentWorker[segments];
        this.currentFutures = new Future[segments];
    }

    public void finishSegment(int segment) {
        synchronized(finished) {
            if (segment > finished.size()) {
                throw new IllegalArgumentException("Invalid segment passed to finishSegment");
            }
            finished.set(segment);
        }
    }

    /**
     * This method gets a segmentedScanResult and submits the next scan request for that segment, if there is one.
     * @return the next available ScanResult
     * @throws ExecutionException if one of the segment pages threw while executing
     * @throws InterruptedException if one of the segment pages was interrupted while executing.
     */
    private ScanContext grab() throws ExecutionException, InterruptedException {
        Future<ScanContext> ret = exec.take();

        ScanRequest originalRequest = ret.get().getScanRequest();
        int segment = originalRequest.getSegment();

        ScanSegmentWorker sw = workers[segment];

        if (sw.hasNext()) {
            currentFutures[segment] = exec.submit(sw);
        } else {
            finishSegment(segment);
            currentFutures[segment] = null;
        }

        return ret.get();
    }

    public void addWorker(ScanSegmentWorker ssw, int segment) {
        workers[segment] = ssw;
        currentFutures[segment] = exec.submit(ssw);
    }

    @Override
    public void close() throws IOException {
        for (int i = 0; i < currentFutures.length; i++) {
            if (currentFutures[i] != null) {
                currentFutures[i].cancel(true);
            }
        }
    }

    @Override
    public boolean hasNext() {
        synchronized (finished) {
            return finished.cardinality() < workers.length;
        }
    }

    @Override
    public ScanContext next() {
        try {
            return grab();
        } catch (ExecutionException e) {
            BackendException backendException = dynamoDBDelegate.unwrapExecutionException(e, DynamoDBDelegate.SCAN);
            throw new BackendRuntimeException(backendException);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove");
    }
}
