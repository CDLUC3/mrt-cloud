/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.cdlib.mrt.s3.service;


import static org.junit.Assert.*;
import org.cdlib.mrt.cloud.object.StateHandler;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;

/**
 *
 * @author dloy
 */
public class NodeIOTest {
    public static final boolean ALPHANUMERIC = true;
    public static final boolean RUNSTATE = false;
    public static final boolean PROCESS = true;
	public static final String KEY = "ark:/99999/test|1|prod/test";
        //public static final String nodeName = "nodes-dev";
    public NodeIOTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }
               
    
    public void test(String nodeName, long nodeNumber, LoggerInf logger) 
        throws Exception
    {
        System.out.println("test:"
                + " - nodeName:" + nodeName
                + " - nodeNumber:" + nodeNumber
        );
        NodeIO nodeIO = null;
        try {
            nodeIO = NodeIO.getNodeIO(nodeName, logger);
            NodeIO.AccessNode accessNode = nodeIO.getAccessNode(nodeNumber);
            if (accessNode == null) {
               throw new TException.REQUESTED_ITEM_NOT_FOUND("test fails:"
                  + " - nodeName:" + nodeName
                  + " - nodeNumber:" + nodeNumber
               );
            }
            CloudStoreInf service = accessNode.service;
            String container = accessNode.container;
            StateHandler stateHandler = StateHandler.getStateHandler(service, container, KEY, logger);
            if (RUNSTATE) {
                StateHandler.RetState result =  stateHandler.process();
                System.out.println(result.dump("NodeIOTest Test " + nodeName + ":" + nodeNumber));
                assertTrue("NodeIOTest Fail Test " + nodeName + ":" + nodeNumber, result.getOk());
            }
            //return new Test(nodeNumber, result);
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }

    public void testGlacier(String nodeName, long nodeNumber, LoggerInf logger) 
        throws Exception
    {
        System.out.println("test:"
                + " - nodeName:" + nodeName
                + " - nodeNumber:" + nodeNumber
        );
        NodeIO nodeIO = null;
        try {
            nodeIO = NodeIO.getNodeIO(nodeName, logger);
            NodeIO.AccessNode accessNode = nodeIO.getAccessNode(nodeNumber);
            if (accessNode == null) {
               throw new TException.REQUESTED_ITEM_NOT_FOUND("test fails:"
                  + " - nodeName:" + nodeName
                  + " - nodeNumber:" + nodeNumber
               );
            }
            CloudStoreInf service = accessNode.service;
            String container = accessNode.container;
            String accessMode = accessNode.accessMode;
            if (StringUtil.isAllBlank(accessMode) || !accessMode.equals("near-line")) {
                throw new TException.INVALID_CONFIGURATION("Glacier content must be 'near-line'");
            }
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    //@Test
    public void testNodeIOUNM()
        throws TException
    {
       
        LoggerInf logger = new TFileLogger("sh", 2, 2);
        try {
            test("nodes-pair-unm", 8101, logger);
            test("nodes-ch-unm", 7301, logger);
            test("nodes-ch-unm", 7101, logger);
            assertTrue(true);
            
        } catch (Exception ex) {
            ex.printStackTrace();
            assertFalse("TestIT exception:" + ex, true);
        }
    }

    @Test
    public void testNodeIOStage()
        throws TException
    {
        String nodeName="nodes-stage";
        LoggerInf logger = new TFileLogger("sh", 2, 2);
        try {
            test(nodeName, 5001, logger);
            test(nodeName, 6001, logger);
            test(nodeName, 9502, logger);
            test(nodeName, 3042, logger);
            test(nodeName, 4101, logger);
            test(nodeName, 7001, logger);
            test(nodeName, 2002, logger);
            testGlacier(nodeName, 6001, logger);
            assertTrue(true);
            
        } catch (Exception ex) {
            ex.printStackTrace();
            assertFalse("TestIT exception:" + ex, true);
        }
    }

    @Test
    public void testNodeIOProd()
        throws TException
    {
        String nodeName="nodes-prod";
        LoggerInf logger = new TFileLogger("sh", 2, 2);
        try {
            test(nodeName, 5001, logger);
            test(nodeName, 6001, logger);
            test(nodeName, 7001, logger);
            test(nodeName, 4001, logger);
            test(nodeName, 3041, logger);
            test(nodeName, 9501, logger);
            test(nodeName, 2001, logger);
            testGlacier(nodeName, 6001, logger);
            assertTrue(true);
            
        } catch (Exception ex) {
            ex.printStackTrace();
            assertFalse("TestIT exception:" + ex, true);
        }
    }
    
    @Test
    public void testNodeIOBadNode()
        throws TException
    {
        LoggerInf logger = new TFileLogger("sh", 2, 2);
        int badNode = 666;
        try {
            test("nodes-dev-store", badNode, logger);
            assertFalse("node should fail:" + badNode,true);
            
        } catch (Exception ex) {
            //ex.printStackTrace();
            assertTrue(true);
        }
    }
    
    @Test
    public void testNodeIOBadTable()
        throws TException
    {
        LoggerInf logger = new TFileLogger("sh", 2, 2);
        String badTable = "nodes-xxx";
        try {
            test(badTable, 5001, logger);
            assertFalse("node table should fail:" + badTable,true);
            
        } catch (Exception ex) {
            //ex.printStackTrace();
            assertTrue(true);
        }
    }



    //@Test (expected=org.cdlib.mrt.utility.TException.INVALID_OR_MISSING_PARM.class)
    public void testException2()
        throws TException
    {
        Identifier id = new Identifier("ABCEDF", null);
    }
}