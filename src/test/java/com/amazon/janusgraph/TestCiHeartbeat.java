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

package com.amazon.janusgraph;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazon.janusgraph.testutils.CiHeartbeat;

/**
 *
 * @author Johan Jacobs
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class TestCiHeartbeat {

    @Rule
    public final TestName testName = new TestName();

    @Mock
    private Appender mockAppender;
    @Captor
    private ArgumentCaptor<LoggingEvent> captorLoggingEvent;

    @Before
    public void setup() {

        final Logger root = Logger.getRootLogger();
        root.addAppender(mockAppender);
        root.setLevel(Level.WARN);
    }

    @Test
    public void testHeartbeatConsoleOutput() throws InterruptedException {

        final CiHeartbeat ciHeartbeat = new CiHeartbeat(500, 3);

        ciHeartbeat.startHeartbeat(testName.getMethodName());

        Thread.sleep(2000);

        ciHeartbeat.stopHeartbeat();

        verify(mockAppender, times(6)).doAppend(captorLoggingEvent.capture());

        final LoggingEvent unitTestStartEvent = captorLoggingEvent.getAllValues().get(0);
        Assert.assertEquals("Heartbeat - [started] - testHeartbeatConsoleOutput - 0ms", unitTestStartEvent.getMessage());

        final LoggingEvent heartbeatOneEvent = captorLoggingEvent.getAllValues().get(1);
        Assert.assertTrue(heartbeatOneEvent.getMessage().toString().contains("Heartbeat - [1] - testHeartbeatConsoleOutput - "));

        final LoggingEvent heartbeatTwoEvent = captorLoggingEvent.getAllValues().get(2);
        Assert.assertTrue(heartbeatTwoEvent.getMessage().toString().contains("Heartbeat - [2] - testHeartbeatConsoleOutput - "));

        final LoggingEvent heartbeatThreeEvent = captorLoggingEvent.getAllValues().get(3);
        Assert.assertTrue(heartbeatThreeEvent.getMessage().toString().contains("Heartbeat - [3] - testHeartbeatConsoleOutput - "));

        final LoggingEvent heartbeatfourEvent = captorLoggingEvent.getAllValues().get(4);
        Assert.assertTrue(heartbeatfourEvent.getMessage().toString().contains("Heartbeat - [4] - testHeartbeatConsoleOutput - "));

        final LoggingEvent unitTestEndEvent = captorLoggingEvent.getAllValues().get(5);
        Assert.assertTrue(unitTestEndEvent.getMessage().toString().contains("Heartbeat - [finished] - testHeartbeatConsoleOutput - "));
    }
}
