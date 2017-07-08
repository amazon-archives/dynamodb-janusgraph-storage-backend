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
package com.amazon.janusgraph.diskstorage.dynamodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.log.Log;
import org.janusgraph.diskstorage.log.LogManager;
import org.janusgraph.diskstorage.log.LogTest;
import org.janusgraph.diskstorage.log.Message;
import org.janusgraph.diskstorage.log.MessageReader;
import org.janusgraph.diskstorage.log.ReadMarker;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.janusgraph.testcategory.MultiIdAuthorityLogStoreCategory;
import com.amazon.janusgraph.testcategory.MultipleItemTestCategory;
import com.google.common.base.Preconditions;

/**
 *
 * @author Alexander Patrikalakis
 * @author Johan Jacobs
 *
*/
@Category({ MultiIdAuthorityLogStoreCategory.class, MultipleItemTestCategory.class })
public class MultiDynamoDBLogTest extends AbstractDynamoDBLogTest {
    public MultiDynamoDBLogTest() throws Exception {
        super(BackendDataModel.MULTI);
    }

    /**
     * TODO remove as soon as new janusgraph-test release allows me to decide the timeout
     * https://github.com/awslabs/dynamodb-titan-storage-backend/issues/160
     * on a per test method basis:
     * https://github.com/JanusGraph/janusgraph/pull/248
     * increase timeoutMs may allow the tests below to pass:
     * mediumSendReceiveSerial
     * testMultipleReadersOnSingleLog
     * testMultipleReadersOnSingleLogSerial
     *
     * https://github.com/awslabs/dynamodb-titan-storage-backend/issues/115
     * Related to issue 160 above, increasing the delay time for each of the following tests
     * allows the tests to pass. Investigation is needed to understand what the effect of increasing the delay
     * has to allow the tests to pass:
     * mediumSendReceiveSerial
     * testMultipleReadersOnSingleLog
     * testMultipleReadersOnSingleLogSerial
     *
     * BEGIN code copied from:
     * https://github.com/JanusGraph/janusgraph/blob/v0.1.0/janusgraph-test/src/main/java/org/janusgraph/diskstorage/log/LogTest.java#L50
     */
    static private final long LONGER_TIMEOUT_MS = 120000;

    @Override
    @Test
    public void mediumSendReceiveSerial() throws Exception {
        simpleSendReceiveMine(2000,50, LONGER_TIMEOUT_MS);
    }
    @Override
    @Test
    public void testMultipleReadersOnSingleLog() throws Exception {
        sendReceiveMine(4, 2000, 50, false, LONGER_TIMEOUT_MS);
    }
    @Override
    @Test
    public void testMultipleReadersOnSingleLogSerial() throws Exception {
        sendReceiveMine(4, 2000, 50, true, LONGER_TIMEOUT_MS);
    }
    private void simpleSendReceiveMine(final int numMessages, final int delayMS, final long timeoutMS) throws Exception {
        sendReceiveMine(1, numMessages, delayMS, true, timeoutMS);
    }
    public void sendReceiveMine(final int readers, final int numMessages, final int delayMS, final boolean expectMessageOrder, final long timeoutMS) throws Exception {
        Preconditions.checkState(0 < readers);

        final Field managerField = LogTest.class.getDeclaredField("manager");
        managerField.setAccessible(true);
        final LogManager myLogManager = (LogManager) managerField.get(this);

        final Log log1 = myLogManager.openLog("test1");
        assertEquals("test1",log1.getName());
        final MyCountingReader[] counts = new MyCountingReader[readers];
        for (int i = 0; i < counts.length; i++) {
            counts[i] = new MyCountingReader(numMessages, expectMessageOrder);
            log1.registerReader(ReadMarker.fromNow(),counts[i]);
        }
        for (long i=1;i<=numMessages;i++) {
            log1.add(BufferUtil.getLongBuffer(i));
            //            System.out.println("Wrote message: " + i);
            Thread.sleep(delayMS);
        }
        for (int i = 0; i < counts.length; i++) {
            final MyCountingReader count = counts[i];
            count.await(timeoutMS);
            assertEquals("counter index " + i + " message count mismatch", numMessages, count.totalMsg.get());
            assertEquals("counter index " + i + " value mismatch", numMessages*(numMessages+1)/2,count.totalValue.get());
            assertTrue(log1.unregisterReader(count));
        }
        log1.close();

    }

    /**
     * Test MessageReader implementation. Allows waiting until an expected number of messages have
     * been read.
     */
    private static class MyLatchMessageReader implements MessageReader {
        private final CountDownLatch latch;

        MyLatchMessageReader(final int expectedMessageCount) {
            latch = new CountDownLatch(expectedMessageCount);
        }

        @Override
        public final void read(final Message message) {
            assertNotNull(message);
            assertNotNull(message.getSenderId());
            assertNotNull(message.getContent());
            final Instant now = Instant.now();
            assertTrue(now.isAfter(message.getTimestamp()) || now.equals(message.getTimestamp()));
            processMessage(message);
            latch.countDown();
        }

        /**
         * Subclasses can override this method to perform additional processing on the message.
         */
        protected void processMessage(final Message message) {}

        /**
         * Blocks until the reader has read the expected number of messages.
         *
         * @param timeoutMillis the maximum time to wait, in milliseconds
         * @throws AssertionError if the specified timeout is exceeded
         */
        public void await(final long timeoutMillis) throws InterruptedException {
            if (latch.await(timeoutMillis, TimeUnit.MILLISECONDS)) {
                return;
            }
            final long c = latch.getCount();
            Preconditions.checkState(0 < c); // TODO remove this, it's not technically correct
            final String msg = "Did not read expected number of messages before timeout was reached (latch count is " + c + ")";
            throw new AssertionError(msg);
        }
    }

    private static class MyCountingReader extends MyLatchMessageReader {

        private static final Logger log =
            LoggerFactory.getLogger(MyCountingReader.class);

        private final AtomicLong totalMsg=new AtomicLong(0);
        private final AtomicLong totalValue=new AtomicLong(0);
        private final boolean expectIncreasingValues;

        private long lastMessageValue = 0;

        private MyCountingReader(final int expectedMessageCount, final boolean expectIncreasingValues) {
            super(expectedMessageCount);
            this.expectIncreasingValues = expectIncreasingValues;
        }

        @Override
        public void processMessage(final Message message) {
            final StaticBuffer content = message.getContent();
            assertEquals(8,content.length());
            final long value = content.getLong(0);
            log.debug("Read log value {} by senderid \"{}\"", value, message.getSenderId());
            if (expectIncreasingValues) {
                assertTrue("Message out of order or duplicated: " + lastMessageValue + " preceded " + value, lastMessageValue<value);
                lastMessageValue = value;
            }
            totalMsg.incrementAndGet();
            totalValue.addAndGet(value);
        }
    }
    //END code copied from https://github.com/JanusGraph/janusgraph/blob/v0.1.0/janusgraph-test/src/main/java/org/janusgraph/diskstorage/log/LogTest.java#L358
}
