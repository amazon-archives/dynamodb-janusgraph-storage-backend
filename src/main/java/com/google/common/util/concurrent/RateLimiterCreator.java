/*
 * Copyright 2014-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Portions copyright 2010 Google, Inc.
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

/**
 * Special RateLimiter constructors and factories are package private in Google Guava.
 * Need to set up the bursting bucket on DynamoDB tables.
 * @author Alexander Patrikalakis
 *
 */
public final class RateLimiterCreator {
    private RateLimiterCreator() {
    }
    /**
     * This method creates a bursting rate limiter with a configurable burst bucket of tokens.
     * @param rate permits per second
     * @param burstBucketSizeInSeconds size of burst bucket expressed in seconds.
     * @return a new rate limiter with the desired burst bucket size.
     */
    //BEGIN copied code
    //https://github.com/google/guava/blob/v18.0/guava/src/com/google/common/util/concurrent/RateLimiter.java#L137
    public static RateLimiter createBurstingLimiter(final double rate, final double burstBucketSizeInSeconds) {
        final RateLimiter rateLimiter = new SmoothRateLimiter.SmoothBursty(RateLimiter.SleepingStopwatch.createFromSystemTimer(),
                burstBucketSizeInSeconds /* maxBurstSeconds */);
        rateLimiter.setRate(rate);
        return rateLimiter;
    }
    //END copied code
    //https://github.com/google/guava/blob/v18.0/guava/src/com/google/common/util/concurrent/RateLimiter.java#L140
}
