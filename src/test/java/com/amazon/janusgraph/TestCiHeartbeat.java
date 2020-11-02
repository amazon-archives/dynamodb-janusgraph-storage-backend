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

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.amazon.janusgraph.testutils.HeartbeatTimerTask;
import com.amazon.janusgraph.testutils.LoggerTestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazon.janusgraph.testutils.CiHeartbeat;

/**
 * Refer to https://www.baeldung.com/logback
 * @author Minghui Tang
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class TestCiHeartbeat {
    @Rule
    public final TestName testName = new TestName();

    private ListAppender<ILoggingEvent> ciHeartbeatListAppender;
    private ListAppender<ILoggingEvent> heartbeatTimerListAppender;
    private CiHeartbeat ciHeartbeat;

    @Before
    public void setup() {
        ciHeartbeatListAppender = LoggerTestUtil.getListAppenderForClass(CiHeartbeat.class);
        heartbeatTimerListAppender = LoggerTestUtil.getListAppenderForClass(HeartbeatTimerTask.class);

        ciHeartbeat = new CiHeartbeat(500, 3);
    }

    @Test
    public void testHeartbeatConsoleOutput() throws InterruptedException{
        // given

        // when
        ciHeartbeat.startHeartbeat(testName.getMethodName());

        Thread.sleep(2000);

        ciHeartbeat.stopHeartbeat();

        // then
        Assert.assertEquals(ciHeartbeatListAppender.list.get(0).getFormattedMessage(), "Heartbeat - [started] - testHeartbeatConsoleOutput - 0ms");
        Assert.assertTrue(heartbeatTimerListAppender.list.get(0).getMessage().contains("Heartbeat - [1] - testHeartbeatConsoleOutput - "));
        Assert.assertTrue(heartbeatTimerListAppender.list.get(1).getMessage().contains("Heartbeat - [2] - testHeartbeatConsoleOutput - "));
        Assert.assertTrue(heartbeatTimerListAppender.list.get(2).getMessage().contains("Heartbeat - [3] - testHeartbeatConsoleOutput - "));
        Assert.assertTrue(ciHeartbeatListAppender.list.get(1).getMessage().contains( "Heartbeat - [finished] - testHeartbeatConsoleOutput - "));
    }
}
