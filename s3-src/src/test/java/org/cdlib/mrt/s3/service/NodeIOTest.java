/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.cdlib.mrt.s3.service;


import static org.junit.Assert.*;
import org.cdlib.mrt.cloud.object.StateHandler;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.utility.LoggerInf;
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
            //StateHandler.RetState result =  stateHandler.process();
            //return new Test(nodeNumber, result);
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
    
    
    @Test
    public void testNodeIODev()
        throws TException
    {
        
        NodeIO nodeIO = null;
        LoggerInf logger = new TFileLogger("sh", 2, 2);
        try {
            test("nodes-dev-store", 910, logger);
            test("nodes-dev-store", 9001, logger);
            test("nodes-dev-store", 5001, logger);
            test("nodes-dev-store", 6001, logger);
            //test("nodes-ch-ucdn", 7013, logger);
            assertTrue(true);
            
        } catch (Exception ex) {
            ex.printStackTrace();
            assertFalse("TestIT exception:" + ex, true);
        }
    }

    @Test
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
        LoggerInf logger = new TFileLogger("sh", 2, 2);
        try {
            test("nodes-stg-store", 2111, logger);
            test("nodes-stg-store", 9502, logger);
            test("nodes-stg-store", 4101, logger);
            test("nodes-stg-store", 3042, logger);
            test("nodes-stg-store", 9901, logger);
            test("nodes-stg-store", 9001, logger);
            test("nodes-stg-store", 5001, logger);
            test("nodes-stg-store", 6001, logger);
            test("nodes-stg-store", 7001, logger);
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
        LoggerInf logger = new TFileLogger("sh", 2, 2);
        try {
            test("nodes-prd-store", 9103, logger);
            test("nodes-prd-store", 3041, logger);
            test("nodes-prd-store", 4001, logger);
            test("nodes-prd-store", 9001, logger);
            test("nodes-prd-store", 5001, logger);
            test("nodes-prd-store", 6001, logger);
            test("nodes-prd-store", 7001, logger);
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