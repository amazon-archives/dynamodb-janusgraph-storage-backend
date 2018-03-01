/*
 * Copyright 2014-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import com.amazon.janusgraph.diskstorage.dynamodb.DynamoDbDelegate;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Class lazily loads all the pages of all the segments of a parallel scan, in multiple threads.
 * @author Alexander Patrikalakis
 *
 */
public class ParallelScanner implements Scanner {
    private final BitSet finished;
    private final ScanSegmentWorker[] workers;
    private final ExecutorCompletionService<ScanContext> exec;
    private final DynamoDbDelegate dynamoDbDelegate;

    // contains all currently running ScanRequests
    private final Future<ScanContext>[] currentFutures;

    public ParallelScanner(final Executor executor, final int segments, final DynamoDbDelegate dynamoDbDelegate) {
        this.dynamoDbDelegate = dynamoDbDelegate;
        this.exec = new ExecutorCompletionService<>(executor);
        this.finished = new BitSet(segments);
        this.finished.clear();
        this.workers = new ScanSegmentWorker[segments];
        this.currentFutures = new Future[segments];
    }

    public void finishSegment(final int segment) {
        synchronized (finished) {
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
        final Future<ScanContext> ret = exec.take();

        final ScanRequest originalRequest = ret.get().getScanRequest();
        final int segment = originalRequest.getSegment();

        final ScanSegmentWorker sw = workers[segment];

        if (sw.hasNext()) {
            currentFutures[segment] = exec.submit(sw);
        } else {
            finishSegment(segment);
            currentFutures[segment] = null;
        }

        //FYI, This might block if nothing is available.
        return ret.get();
    }

    public void addWorker(final ScanSegmentWorker ssw, final int segment) {
        workers[segment] = ssw;
        currentFutures[segment] = exec.submit(ssw);
    }

    @Override
    public void close() throws IOException {
        for (Future<ScanContext> currentFuture : currentFutures) {
            if (currentFuture != null) {
                currentFuture.cancel(true);
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
    @SuppressFBWarnings(value = "IT_NO_SUCH_ELEMENT",
        justification = "https://github.com/awslabs/dynamodb-janusgraph-storage-backend/issues/222")
    public ScanContext next() {
        try {
            return grab();
        } catch (ExecutionException e) {
            final BackendException backendException = dynamoDbDelegate.unwrapExecutionException(e, DynamoDbDelegate.SCAN);
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
