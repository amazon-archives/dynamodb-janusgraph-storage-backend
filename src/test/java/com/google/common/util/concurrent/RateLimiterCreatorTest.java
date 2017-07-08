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
package com.google.common.util.concurrent;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

/**
 *
 * @author Alexander Patrikalakis
 */
public class RateLimiterCreatorTest {

    static final double DELTA = 0.1;

    @Test
    public void testTrickleBurst() {
        final RateLimiter l = RateLimiterCreator.createBurstingLimiter(1 /*rate*/,1 /*burstBucketSize*/);
        double waited = l.acquire(1);
        assertEquals(0.0, waited, DELTA);
        waited = l.acquire(1);
        assertEquals(1.0, waited, DELTA);
        Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);
        waited = l.acquire(5);
        assertEquals(0.0, waited, DELTA);
    }
}
