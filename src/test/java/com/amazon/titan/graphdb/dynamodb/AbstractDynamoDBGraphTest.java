/*
 * Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Portions copyright Titan: Distributed Graph Database - Copyright 2012 and onwards Aurelius.
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
package com.amazon.titan.graphdb.dynamodb;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.LOG_BACKEND;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.LOG_READ_INTERVAL;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.LOG_SEND_DELAY;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.MAX_COMMIT_TIME;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.SYSTEM_LOG_TRANSACTIONS;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.TRANSACTION_LOG;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.USER_LOG;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.VERBOSE_TX_RECOVERY;
import static com.tinkerpop.blueprints.Direction.BOTH;
import static com.tinkerpop.blueprints.Direction.OUT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.rules.TestName;

import com.amazon.titan.diskstorage.dynamodb.BackendDataModel;
import com.amazon.titan.diskstorage.dynamodb.test.TestGraphUtil;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.attribute.Decimal;
import com.thinkaurelius.titan.core.log.Change;
import com.thinkaurelius.titan.core.log.ChangeProcessor;
import com.thinkaurelius.titan.core.log.ChangeState;
import com.thinkaurelius.titan.core.log.LogProcessorFramework;
import com.thinkaurelius.titan.core.log.TransactionId;
import com.thinkaurelius.titan.core.log.TransactionRecovery;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.diskstorage.log.Log;
import com.thinkaurelius.titan.diskstorage.log.Message;
import com.thinkaurelius.titan.diskstorage.log.MessageReader;
import com.thinkaurelius.titan.diskstorage.log.ReadMarker;
import com.thinkaurelius.titan.diskstorage.log.kcvs.KCVSLog;
import com.thinkaurelius.titan.diskstorage.util.time.StandardDuration;
import com.thinkaurelius.titan.diskstorage.util.time.Timepoint;
import com.thinkaurelius.titan.diskstorage.util.time.TimestampProvider;
import com.thinkaurelius.titan.graphdb.TestMockLog;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;
import com.thinkaurelius.titan.graphdb.database.EdgeSerializer;
import com.thinkaurelius.titan.graphdb.database.log.LogTxMeta;
import com.thinkaurelius.titan.graphdb.database.log.LogTxStatus;
import com.thinkaurelius.titan.graphdb.database.log.TransactionLogHeader;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.log.StandardTransactionLogProcessor;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;

/**
 *
 * FunctionalTitanGraphTest contains the specializations of the Titan functional tests required for
 * the DynamoDB storage backend.
 *
 * @author Alexander Patrikalakis
 *
 */
public abstract class AbstractDynamoDBGraphTest extends TitanGraphTest {
    @Rule public TestName name = new TestName();

    @Override
    protected boolean isLockingOptimistic() {
        return true;
    }

    public abstract BackendDataModel getDataModel() ;

    @Override
    public WriteConfiguration getConfiguration() {
        final String methodName = name.getMethodName();
        final List<String> extraStoreNames =
            methodName.contains("simpleLogTest") ? Collections.singletonList(TestGraphUtil.instance().getUserLogName("test")) : Collections.<String>emptyList();
        return TestGraphUtil.instance().getWriteConfiguration(getDataModel(), extraStoreNames);
    }

    //if metrics with prefix are turned on, the log tests fail,
    //so condition the assertNull on metricsPrefix also being null
    //begin titan-test code
    //https://github.com/thinkaurelius/titan/blob/0.5.4/titan-test/src/main/java/com/thinkaurelius/titan/graphdb/TitanGraphTest.java#L3310
    @Override
    public void simpleLogTest(final boolean withLogFailure) throws InterruptedException {
        final String userlogName = "test";
        final Serializer serializer = graph.getDataSerializer();
        final EdgeSerializer edgeSerializer = graph.getEdgeSerializer();
        final TimestampProvider times = graph.getConfiguration().getTimestampProvider();
        final TimeUnit unit = times.getUnit();
        final long startTime = times.getTime().getTimestamp(TimeUnit.MILLISECONDS);
        clopen( option(SYSTEM_LOG_TRANSACTIONS), true,
                option(LOG_BACKEND, USER_LOG),(withLogFailure?TestMockLog.class.getName():LOG_BACKEND.getDefaultValue()),
                option(TestMockLog.LOG_MOCK_FAILADD, USER_LOG),withLogFailure,
                option(KCVSLog.LOG_READ_LAG_TIME, USER_LOG),new StandardDuration(50,TimeUnit.MILLISECONDS),
                option(LOG_READ_INTERVAL, USER_LOG),new StandardDuration(250,TimeUnit.MILLISECONDS),
                option(LOG_SEND_DELAY, USER_LOG),new StandardDuration(100,TimeUnit.MILLISECONDS),
                option(KCVSLog.LOG_READ_LAG_TIME,TRANSACTION_LOG),new StandardDuration(50,TimeUnit.MILLISECONDS),
                option(LOG_READ_INTERVAL,TRANSACTION_LOG),new StandardDuration(250,TimeUnit.MILLISECONDS),
                option(MAX_COMMIT_TIME),new StandardDuration(1,TimeUnit.SECONDS)
        );
        final String instanceid = graph.getConfiguration().getUniqueGraphId();

        PropertyKey weight = tx.makePropertyKey("weight").dataType(Decimal.class).cardinality(Cardinality.SINGLE).make();
        EdgeLabel knows = tx.makeEdgeLabel("knows").make();
        TitanVertex n1 = tx.addVertex();
        n1.addProperty(weight, 10.5);
        newTx();

        final Timepoint txTimes[] = new Timepoint[4];
        //Transaction with custom userlog name
        txTimes[0] = times.getTime();
        TitanTransaction tx2 = graph.buildTransaction().setLogIdentifier(userlogName).start();
        TitanVertex v1 = tx2.addVertex();
        v1.setProperty("weight", 111.1);
        v1.addEdge("knows", v1);
        tx2.commit();
        final long v1id = v1.getLongId();
        txTimes[1] = times.getTime();
        tx2 = graph.buildTransaction().setLogIdentifier(userlogName).start();
        TitanVertex v2 = tx2.addVertex();
        v2.setProperty("weight",222.2);
        v2.addEdge("knows",tx2.getVertex(v1id));
        tx2.commit();
        final long v2id = v2.getLongId();
        //Only read tx
        tx2 = graph.buildTransaction().setLogIdentifier(userlogName).start();
        v1 = tx2.getVertex(v1id);
        assertEquals(111.1,v1.<Decimal>getProperty("weight").doubleValue(),0.0);
        assertEquals(222.2,tx2.getVertex(v2).<Decimal>getProperty("weight").doubleValue(),0.0);
        tx2.commit();
        //Deleting transaction
        txTimes[2] = times.getTime();
        tx2 = graph.buildTransaction().setLogIdentifier(userlogName).start();
        v2 = tx2.getVertex(v2id);
        assertEquals(222.2,v2.<Decimal>getProperty("weight").doubleValue(),0.0);
        v2.remove();
        tx2.commit();
        //Edge modifying transaction
        txTimes[3] = times.getTime();
        tx2 = graph.buildTransaction().setLogIdentifier(userlogName).start();
        v1 = tx2.getVertex(v1id);
        assertEquals(111.1,v1.<Decimal>getProperty("weight").doubleValue(),0.0);
        Edge e = Iterables.getOnlyElement(v1.getEdges(Direction.OUT,"knows"));
        assertNull(e.getProperty("weight"));
        e.setProperty("weight",44.4);
        tx2.commit();
        close();
        final long endTime = times.getTime().getTimestamp(TimeUnit.MILLISECONDS);

        final ReadMarker startMarker = ReadMarker.fromTime(startTime, TimeUnit.MILLISECONDS);
        final String metricsPrefix = graph.getConfiguration().getMetricsPrefix();//added for groupname test

        Log txlog = openTxLog();
        Log userLog = openUserLog(userlogName);
        final EnumMap<LogTxStatus,AtomicInteger> txMsgCounter = new EnumMap<LogTxStatus,AtomicInteger>(LogTxStatus.class);
        for (LogTxStatus status : LogTxStatus.values()) txMsgCounter.put(status,new AtomicInteger(0));
        final AtomicInteger userlogMeta = new AtomicInteger(0);
        txlog.registerReader(startMarker,new MessageReader() {
            @Override
            public void read(Message message) {
                long msgTime = message.getTimestamp(TimeUnit.MILLISECONDS);
                assertTrue(msgTime>=startTime);
                assertNotNull(message.getSenderId());
                TransactionLogHeader.Entry txEntry = TransactionLogHeader.parse(message.getContent(),serializer, times);
                TransactionLogHeader header = txEntry.getHeader();
//                System.out.println(header.getTimestamp(TimeUnit.MILLISECONDS));
                assertTrue(header.getTimestamp(TimeUnit.MILLISECONDS) >= startTime);
                assertTrue(header.getTimestamp(TimeUnit.MILLISECONDS)<=msgTime);
                assertNotNull(txEntry.getMetadata());
                // handle case where prefix is null or not null - changed original simpleLogTest(boolean)
                final Object groupnameObj = txEntry.getMetadata().get(LogTxMeta.GROUPNAME);
                final String groupname = groupnameObj == null ? null : groupnameObj.toString();
                if(metricsPrefix == null) {
                    assertNull(groupname);
                }
                LogTxStatus status = txEntry.getStatus();
                if (status==LogTxStatus.PRECOMMIT) {
                    assertTrue(txEntry.hasContent());
                    Object logid = txEntry.getMetadata().get(LogTxMeta.LOG_ID);
                    if (logid!=null) {
                        assertTrue(logid instanceof String);
                        assertEquals(userlogName,logid);
                        userlogMeta.incrementAndGet();
                    }
                } else if (withLogFailure) {
                    assertTrue(status.isPrimarySuccess() || status==LogTxStatus.SECONDARY_FAILURE);
                    if (status==LogTxStatus.SECONDARY_FAILURE) {
                        TransactionLogHeader.SecondaryFailures secFail = txEntry.getContentAsSecondaryFailures(serializer);
                        assertTrue(secFail.failedIndexes.isEmpty());
                        assertTrue(secFail.userLogFailure);
                    }
                } else {
                    assertFalse(txEntry.hasContent());
                    assertTrue(status.isSuccess());
                }
                txMsgCounter.get(txEntry.getStatus()).incrementAndGet();
            }
        });
        final EnumMap<Change,AtomicInteger> userChangeCounter = new EnumMap<Change,AtomicInteger>(Change.class);
        for (Change change : Change.values()) userChangeCounter.put(change,new AtomicInteger(0));
        final AtomicInteger userLogMsgCounter = new AtomicInteger(0);
        userLog.registerReader(startMarker, new MessageReader() {
            @Override
            public void read(Message message) {
                long msgTime = message.getTimestamp(TimeUnit.MILLISECONDS);
                assertTrue(msgTime >= startTime);
                assertNotNull(message.getSenderId());
                StaticBuffer content = message.getContent();
                assertTrue(content != null && content.length() > 0);
                TransactionLogHeader.Entry txentry = TransactionLogHeader.parse(content, serializer, times);

                long txTime = txentry.getHeader().getTimestamp(TimeUnit.MILLISECONDS);
                assertTrue(txTime <= msgTime);
                assertTrue(txTime >= startTime);
                long txid = txentry.getHeader().getId();
                assertTrue(txid > 0);
                for (TransactionLogHeader.Modification modification : txentry.getContentAsModifications(serializer)) {
                    assertTrue(modification.state == Change.ADDED || modification.state == Change.REMOVED);
                    userChangeCounter.get(modification.state).incrementAndGet();
                }
                userLogMsgCounter.incrementAndGet();
            }
        });
        Thread.sleep(4000);
        assertEquals(5,txMsgCounter.get(LogTxStatus.PRECOMMIT).get());
        assertEquals(4,txMsgCounter.get(LogTxStatus.PRIMARY_SUCCESS).get());
        assertEquals(1,txMsgCounter.get(LogTxStatus.COMPLETE_SUCCESS).get());
        assertEquals(4, userlogMeta.get());
        if (withLogFailure) assertEquals(4,txMsgCounter.get(LogTxStatus.SECONDARY_FAILURE).get());
        else assertEquals(4,txMsgCounter.get(LogTxStatus.SECONDARY_SUCCESS).get());
        //User-Log
        if (withLogFailure) {
            assertEquals(0, userLogMsgCounter.get());
        } else {
            assertEquals(4, userLogMsgCounter.get());
            assertEquals(7, userChangeCounter.get(Change.ADDED).get());
            assertEquals(4,userChangeCounter.get(Change.REMOVED).get());
        }

        clopen( option(VERBOSE_TX_RECOVERY), true );
        /*
        Transaction Recovery
         */
        TransactionRecovery recovery = TitanFactory.startTransactionRecovery(graph,startTime,TimeUnit.MILLISECONDS);


        /*
        Use user log processing framework
         */
        final AtomicInteger userLogCount = new AtomicInteger(0);
        LogProcessorFramework userlogs = TitanFactory.openTransactionLog(graph);
        userlogs.addLogProcessor(userlogName).setStartTime(startTime, TimeUnit.MILLISECONDS).setRetryAttempts(1)
        .addProcessor(new ChangeProcessor() {
            @Override
            public void process(TitanTransaction tx, TransactionId txId, ChangeState changes) {
                assertEquals(instanceid,txId.getInstanceId());
                assertTrue(txId.getTransactionId()>0 && txId.getTransactionId()<100); //Just some reasonable upper bound
                final long txTime = txId.getTransactionTime().sinceEpoch(TimeUnit.MILLISECONDS);
                assertTrue(String.format("tx timestamp %s not between start %s and end time %s",
                        txTime,startTime,endTime),
                        txTime>=startTime && txTime<=endTime); //Times should be within a second

                assertTrue(tx.containsRelationType("knows"));
                assertTrue(tx.containsRelationType("weight"));
                EdgeLabel knows = tx.getEdgeLabel("knows");
                PropertyKey weight = tx.getPropertyKey("weight");

                long txTimeMicro = txId.getTransactionTime().sinceEpoch(TimeUnit.MICROSECONDS);

                int txNo;
                if (txTimeMicro<txTimes[1].getTimestamp(TimeUnit.MICROSECONDS)) {
                    txNo=1;
                    //v1 addition transaction
                    assertEquals(1, Iterables.size(changes.getVertices(Change.ADDED)));
                    assertEquals(0, Iterables.size(changes.getVertices(Change.REMOVED)));
                    assertEquals(1,Iterables.size(changes.getVertices(Change.ANY)));
                    assertEquals(2,Iterables.size(changes.getRelations(Change.ADDED)));
                    assertEquals(1,Iterables.size(changes.getRelations(Change.ADDED, knows)));
                    assertEquals(1,Iterables.size(changes.getRelations(Change.ADDED, weight)));
                    assertEquals(2,Iterables.size(changes.getRelations(Change.ANY)));
                    assertEquals(0,Iterables.size(changes.getRelations(Change.REMOVED)));

                    TitanVertex v = Iterables.getOnlyElement(changes.getVertices(Change.ADDED));
                    assertEquals(v1id,v.getLongId());
                    TitanProperty p = Iterables.getOnlyElement(changes.getProperties(v,Change.ADDED,"weight"));
                    assertEquals(111.1,p.<Decimal>getValue().doubleValue(),0.0001);
                    assertEquals(1,Iterables.size(changes.getEdges(v, Change.ADDED, OUT)));
                    assertEquals(1,Iterables.size(changes.getEdges(v, Change.ADDED, BOTH)));
                } else if (txTimeMicro<txTimes[2].getTimestamp(TimeUnit.MICROSECONDS)) {
                    txNo=2;
                    //v2 addition transaction
                    assertEquals(1, Iterables.size(changes.getVertices(Change.ADDED)));
                    assertEquals(0, Iterables.size(changes.getVertices(Change.REMOVED)));
                    assertEquals(2,Iterables.size(changes.getVertices(Change.ANY)));
                    assertEquals(2,Iterables.size(changes.getRelations(Change.ADDED)));
                    assertEquals(1,Iterables.size(changes.getRelations(Change.ADDED, knows)));
                    assertEquals(1,Iterables.size(changes.getRelations(Change.ADDED, weight)));
                    assertEquals(2,Iterables.size(changes.getRelations(Change.ANY)));
                    assertEquals(0,Iterables.size(changes.getRelations(Change.REMOVED)));

                    TitanVertex v = Iterables.getOnlyElement(changes.getVertices(Change.ADDED));
                    assertEquals(v2id,v.getLongId());
                    TitanProperty p = Iterables.getOnlyElement(changes.getProperties(v,Change.ADDED,"weight"));
                    assertEquals(222.2,p.<Decimal>getValue().doubleValue(),0.0001);
                    assertEquals(1,Iterables.size(changes.getEdges(v, Change.ADDED, OUT)));
                    assertEquals(1,Iterables.size(changes.getEdges(v, Change.ADDED, BOTH)));
                } else if (txTimeMicro<txTimes[3].getTimestamp(TimeUnit.MICROSECONDS)) {
                    txNo=3;
                    //v2 deletion transaction
                    assertEquals(0, Iterables.size(changes.getVertices(Change.ADDED)));
                    assertEquals(1, Iterables.size(changes.getVertices(Change.REMOVED)));
                    assertEquals(2,Iterables.size(changes.getVertices(Change.ANY)));
                    assertEquals(0,Iterables.size(changes.getRelations(Change.ADDED)));
                    assertEquals(2,Iterables.size(changes.getRelations(Change.REMOVED)));
                    assertEquals(1,Iterables.size(changes.getRelations(Change.REMOVED, knows)));
                    assertEquals(1,Iterables.size(changes.getRelations(Change.REMOVED, weight)));
                    assertEquals(2,Iterables.size(changes.getRelations(Change.ANY)));

                    TitanVertex v = Iterables.getOnlyElement(changes.getVertices(Change.REMOVED));
                    assertEquals(v2id,v.getLongId());
                    TitanProperty p = Iterables.getOnlyElement(changes.getProperties(v,Change.REMOVED,"weight"));
                    assertEquals(222.2,p.<Decimal>getValue().doubleValue(),0.0001);
                    assertEquals(1,Iterables.size(changes.getEdges(v, Change.REMOVED, OUT)));
                    assertEquals(0,Iterables.size(changes.getEdges(v, Change.ADDED, BOTH)));
                } else {
                    txNo=4;
                    //v1 edge modification
                    assertEquals(0, Iterables.size(changes.getVertices(Change.ADDED)));
                    assertEquals(0, Iterables.size(changes.getVertices(Change.REMOVED)));
                    assertEquals(1,Iterables.size(changes.getVertices(Change.ANY)));
                    assertEquals(1,Iterables.size(changes.getRelations(Change.ADDED)));
                    assertEquals(1,Iterables.size(changes.getRelations(Change.REMOVED)));
                    assertEquals(1,Iterables.size(changes.getRelations(Change.REMOVED, knows)));
                    assertEquals(2,Iterables.size(changes.getRelations(Change.ANY)));

                    TitanVertex v = Iterables.getOnlyElement(changes.getVertices(Change.ANY));
                    assertEquals(v1id,v.getLongId());
                    TitanEdge e = Iterables.getOnlyElement(changes.getEdges(v,Change.REMOVED,Direction.OUT,"knows"));
                    assertNull(e.getProperty("weight"));
                    assertEquals(v,e.getVertex(Direction.IN));
                    e = Iterables.getOnlyElement(changes.getEdges(v,Change.ADDED,Direction.OUT,"knows"));
                    assertEquals(44.4,e.<Decimal>getProperty("weight").doubleValue(),0.0);
                    assertEquals(v,e.getVertex(Direction.IN));
                }

                //See only current state of graph in transaction
                TitanVertex v1 = tx.getVertex(v1id);
                assertNotNull(v1);
                assertTrue(v1.isLoaded());
                TitanVertex v2 = tx.getVertex(v2id);
                if (txNo!=2) {
                    //In the transaction that adds v2, v2 will be considered "loaded"
                    assertTrue(txNo+ " - " + v2,v2==null || v2.isRemoved());
                }
                assertEquals(111.1,v1.<Decimal>getProperty(weight).doubleValue(),0.0);
                assertEquals(1,Iterables.size(v1.getEdges(Direction.OUT)));

                userLogCount.incrementAndGet();
            }
        }).build();

        //wait
        Thread.sleep(22000L);

        recovery.shutdown();
        long[] recoveryStats = ((StandardTransactionLogProcessor)recovery).getStatistics();
        if (withLogFailure) {
            assertEquals(1,recoveryStats[0]);
            assertEquals(4,recoveryStats[1]);
        } else {
            assertEquals(5,recoveryStats[0]);
            assertEquals(0,recoveryStats[1]);

        }

        userlogs.removeLogProcessor(userlogName);
        userlogs.shutdown();
        assertEquals(4, userLogCount.get());
    }
    //end titan-test code

    @AfterClass
    public static void deleteTables() throws BackendException {
        TestGraphUtil.cleanUpTables();
    }
}
