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
package com.amazon.titan.graphdb.dynamodb;

import java.util.concurrent.ExecutionException;

import com.amazon.titan.testutils.TravisCiHeartbeat;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.amazon.titan.diskstorage.dynamodb.BackendDataModel;
import com.amazon.titan.testcategory.IsolateMultiEdgesExceedCacheSize;
import com.amazon.titan.testcategory.IsolateMultiLargeJointIndexRetrieval;
import com.amazon.titan.testcategory.IsolateMultiVertexCentricQuery;
import com.amazon.titan.testcategory.MultiDynamoDBGraphTestCategory;
import com.thinkaurelius.titan.diskstorage.BackendException;
import org.junit.rules.TestName;

/**
 * @author Alexander Patrikalakis
 */
public class MultiDynamoDBGraphTest extends AbstractDynamoDBGraphTest {

    public MultiDynamoDBGraphTest() {
        super(BackendDataModel.MULTI);
    }

    //TODO in super superclass TitanGraphTest make sure exception signature resolves to something
    //more specific than Exception.class

    //$ cat TEST-com.amazon.titan.graphdb.dynamodb.MultiDynamoDBGraphTest.xml | grep testcase | sed \
    // 's/.*\ name=\"\([^\"]*\)\".*/\ \ \ \ @Category({\ MultiDynamoDBGraphTestCategory.class\ })\ public\ void\ \1()\ {\ super\.\1();\ }/'

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testOpenClose() {
        super.testOpenClose();
    }

    @Test
    @Override
    @Category({IsolateMultiLargeJointIndexRetrieval.class})
    public void testLargeJointIndexRetrieval() {
        super.testLargeJointIndexRetrieval();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testMediumCreateRetrieve() {
        super.testMediumCreateRetrieve();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testSettingTTLOnUnsupportedType() throws Exception {
        super.testSettingTTLOnUnsupportedType();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testSchemaNameChange() {
        super.testSchemaNameChange();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void simpleLogTest() throws InterruptedException {
        super.simpleLogTest();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testSchemaTypes() {
        super.testSchemaTypes();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testTinkerPopOptimizationStrategies() {
        super.testTinkerPopOptimizationStrategies();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testGetTTLFromUnsupportedType() throws Exception {
        super.testGetTTLFromUnsupportedType();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testLocalGraphConfiguration() {
        super.testLocalGraphConfiguration();
    }

    @Test
    @Override
    @Category({IsolateMultiVertexCentricQuery.class})
    public void testVertexCentricQuery() {
        super.testVertexCentricQuery();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testConcurrentConsistencyEnforcement() throws Exception {
        super.testConcurrentConsistencyEnforcement();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testTransactionalScopeOfSchemaTypes() {
        super.testTransactionalScopeOfSchemaTypes();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testNestedTransactions() {
        super.testNestedTransactions();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testBasic() {
        super.testBasic();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testUnsettingTTL() throws InterruptedException {
        super.testUnsettingTTL();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testGlobalOfflineGraphConfig() {
        super.testGlobalOfflineGraphConfig();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testLimitWithMixedIndexCoverage() {
        super.testLimitWithMixedIndexCoverage();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testMultivaluedVertexProperty() {
        super.testMultivaluedVertexProperty();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testGlobalGraphConfig() {
        super.testGlobalGraphConfig();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testManagedOptionMasking() throws BackendException {
        super.testManagedOptionMasking();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testGlobalGraphIndexingAndQueriesForInternalIndexes() {
        super.testGlobalGraphIndexingAndQueriesForInternalIndexes();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testWithoutIndex() {
        super.testWithoutIndex();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testIndexUpdatesWithReindexAndRemove() throws InterruptedException, ExecutionException {
        super.testIndexUpdatesWithReindexAndRemove();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testEdgeTTLTiming() throws Exception {
        super.testEdgeTTLTiming();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testStaleVertex() {
        super.testStaleVertex();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testGettingUndefinedVertexLabelTTL() {
        super.testGettingUndefinedVertexLabelTTL();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void simpleLogTestWithFailure() throws InterruptedException {
        super.simpleLogTestWithFailure();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testVertexCentricIndexWithNull() {
        super.testVertexCentricIndexWithNull();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testVertexTTLImplicitKey() throws Exception {
        super.testVertexTTLImplicitKey();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testImplicitKey() {
        super.testImplicitKey();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testMaskableGraphConfig() {
        super.testMaskableGraphConfig();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testDataTypes() throws Exception {
        super.testDataTypes();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testEdgeTTLImplicitKey() throws Exception {
        super.testEdgeTTLImplicitKey();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testTinkerPopCardinality() {
        super.testTinkerPopCardinality();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testPropertyCardinality() {
        super.testPropertyCardinality();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testArrayEqualityUsingImplicitKey() {
        super.testArrayEqualityUsingImplicitKey();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testFixedGraphConfig() {
        super.testFixedGraphConfig();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testAutomaticTypeCreation() {
        super.testAutomaticTypeCreation();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testGettingUndefinedEdgeLabelTTL() {
        super.testGettingUndefinedEdgeLabelTTL();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testSimpleTinkerPopTraversal() {
        super.testSimpleTinkerPopTraversal();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testGlobalIteration() {
        super.testGlobalIteration();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testVertexRemoval() {
        super.testVertexRemoval();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testForceIndexUsage() {
        super.testForceIndexUsage();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testSettingTTLOnNonStaticVertexLabel() throws Exception {
        super.testSettingTTLOnNonStaticVertexLabel();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testTransactionConfiguration() {
        super.testTransactionConfiguration();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testConsistencyEnforcement() {
        super.testConsistencyEnforcement();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testHasNot() {
        super.testHasNot();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testVertexTTLWithCompositeIndex() throws Exception {
        super.testVertexTTLWithCompositeIndex();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testRelationTypeIndexes() {
        super.testRelationTypeIndexes();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testGotGIndexRemoval() throws InterruptedException, ExecutionException {
        super.testGotGIndexRemoval();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testTransactionIsolation() {
        super.testTransactionIsolation();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testSelfLoop() {
        super.testSelfLoop();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testIndexUniqueness() {
        super.testIndexUniqueness();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testEdgeTTLWithTransactions() throws Exception {
        super.testEdgeTTLWithTransactions();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testIndexQueryWithLabelsAndContainsIN() {
        super.testIndexQueryWithLabelsAndContainsIN();
    }

    @Test
    @Override
    @Category({IsolateMultiEdgesExceedCacheSize.class})
    public void testEdgesExceedCacheSize() {
        super.testEdgesExceedCacheSize();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testThreadBoundTx() {
        super.testThreadBoundTx();
    }

    @Test
    @Override
    @Category({MultiDynamoDBGraphTestCategory.class})
    public void testCreateDelete() {
        super.testCreateDelete();
    }
}
