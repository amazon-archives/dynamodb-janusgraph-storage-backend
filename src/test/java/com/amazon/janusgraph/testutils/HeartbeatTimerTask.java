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

package com.amazon.janusgraph.testutils;

import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Johan Jacobs
 *
 */
public class HeartbeatTimerTask extends TimerTask {

    private static final Logger LOG = LoggerFactory.getLogger(HeartbeatTimerTask.class);

    private int intervals;
    private final int configuredMaximumIntervals;
    private final long heartbeatStartTime;
    private final String configuredUnitTestName;

    public HeartbeatTimerTask(final int configuredMaximumIntervals, final long heartbeatStartTime, final String configuredUnitTestName) {
        this.configuredMaximumIntervals = configuredMaximumIntervals;
        this.heartbeatStartTime = heartbeatStartTime;
        this.configuredUnitTestName = configuredUnitTestName;
    }

    @Override
    public void run() {

        intervals++;
        final long currentRunTimeMilliseconds = System.currentTimeMillis() - heartbeatStartTime;

        if (intervals == configuredMaximumIntervals) {
            LOG.warn(String.format("Heartbeat - [%d] - %s - %dms.",
                    intervals,
                    configuredUnitTestName,
                    currentRunTimeMilliseconds));
            return;
        }

        LOG.info(String.format("Heartbeat - [%d] - %s - %dms",
                intervals,
                configuredUnitTestName,
                currentRunTimeMilliseconds));
    }
}
