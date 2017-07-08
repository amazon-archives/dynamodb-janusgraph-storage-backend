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

import java.util.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This heartbeat timer will print to the console every x configured milliseconds. This is to prevent Travis CI from
 * terminating the build if there is no console output in a 10min time frame. The heartbeat will only run for up to x
 * configured intervals, this is needed for tests which are really in a hung state and Travis CI can then fail the build.
 *
 * The heartbeat is started and stopped before and after each test case. The current running unit test and the current
 * execution time of the test is included in the heartbeat for better visibility in to any long running tests.
 *
 * @author Johan Jacobs
 *
 */
public class CiHeartbeat {

    private static final Logger LOG = LoggerFactory.getLogger(CiHeartbeat.class);

    private static final long DEFAULT_HEARTBEAT_INTERVAL    = 300000;
    private static final int DEFAULT_MAXIMUM_INTERVALS      = 8;

    private Timer heartbeatTimer;
    private boolean timerStarted;
    private final long configuredHeartbeatInterval;
    private final int configuredMaximumIntervals;
    private String configuredUnitTestName;
    private final long heartbeatStartTime;

    public CiHeartbeat() {
        this(DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_MAXIMUM_INTERVALS);
    }

    public CiHeartbeat(final long heartbeatInterval, final int maximumIntervals) {

        this.configuredHeartbeatInterval = heartbeatInterval;
        this.configuredMaximumIntervals = maximumIntervals;
        this.heartbeatStartTime = System.currentTimeMillis();
    }

    public void startHeartbeat(final String unitTestName) {

        if(this.timerStarted) {
            LOG.warn(String.format("Travis CI heartbeat timer is already running for unit test with name: %s.", this.configuredUnitTestName));
            return;
        }

        this.configuredUnitTestName = unitTestName;
        this.heartbeatTimer = new Timer("Unit test heartbeat timer");

        final HeartbeatTimerTask heartbeatTimerTask = new HeartbeatTimerTask(this.configuredMaximumIntervals, this.heartbeatStartTime, this.configuredUnitTestName);
        this.heartbeatTimer.schedule(heartbeatTimerTask, this.configuredHeartbeatInterval, this.configuredHeartbeatInterval);

        this.timerStarted = true;

        LOG.info(String.format("Heartbeat - [started] - %s - 0ms",
                this.configuredUnitTestName));
    }

    public void stopHeartbeat() {

        if (this.heartbeatTimer != null && this.timerStarted) {
            this.heartbeatTimer.cancel();
            this.heartbeatTimer = null;
            this.timerStarted = false;

            final long currentRunTimeMilliseconds = System.currentTimeMillis() - heartbeatStartTime;

            LOG.info(String.format("Heartbeat - [finished] - %s - %dms",
                    configuredUnitTestName,
                    currentRunTimeMilliseconds));
        }
    }
}
