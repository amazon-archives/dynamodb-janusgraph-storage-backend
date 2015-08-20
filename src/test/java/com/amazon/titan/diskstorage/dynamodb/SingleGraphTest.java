package com.amazon.titan.diskstorage.dynamodb;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.amazon.titan.diskstorage.dynamodb.test.TestGraphUtil;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.StorageException;

/**
 *
 * @author Matthew Sowders
 * @author Alexander Patrikalakis
 */
public class SingleGraphTest extends AbstractGraphTestBase {

    private static TitanGraph GRAPH = null;

    @BeforeClass
    public static void setUpGraph() {
        GRAPH = TestGraphUtil.instance().openGraph(BackendDataModel.SINGLE);
        createSchema(GRAPH);
    }

    @AfterClass
    public static void tearDownGraph() throws StorageException {
        TestGraphUtil.tearDownGraph(GRAPH);
    }

    @Override
    protected TitanGraph getGraph()
    {
        return GRAPH;
    }
}
