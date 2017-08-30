/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.cdlib.mrt.s3.service;

import org.cdlib.mrt.s3.service.CloudUtil;
import org.cdlib.mrt.core.Identifier;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.cdlib.mrt.utility.TException;

/**
 *
 * @author dloy
 */
public class CloudUtilTest {
    public static final boolean ALPHANUMERIC = true;
    public CloudUtilTest() {
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

    // TODO add test methods here.
    // The methods must be annotated with annotation @Test. For example:
    //
    // @Test
    // public void hello() {}


    @Test
    public void TestIt()
        throws TException
    {
        TestIt("Test1", "ark:/13030/abcde", "2", "part1/part2", true);
        TestIt("Test2", "ark:/13030/abcde", "2", "part1/part2", false);
        TestIt("Test3", "ark:/13030/abcde", "3", null, true);
        TestIt("Test4", "ark:/13030/abcde", "3", null, false);
        TestIt("Test5", "ark:/13030/abcde", null, null, true);
        TestIt("Test6", "ark:/13030/abcde", null, null, false);
    }
    
    public void TestIt(
            String header,
            String objectIDS, 
            String versionIDS, 
            String fileID, 
            boolean alphaNumeric)
        throws TException
    {
        try {
            System.out.println("***" + header + "***");
            Identifier objectID = new Identifier(objectIDS);
            Integer versionID = null;
            if (versionIDS != null) {
                versionID = Integer.parseInt(versionIDS);
            }
            String key = CloudUtil.getKey(objectID, versionID, fileID, alphaNumeric);
            System.out.println("alpha=" + alphaNumeric);
            System.out.println("eKey=" + key);
            System.out.println("dKey=" + CloudUtil.decodeElement(key));
            //assertTrue(key.equals("ark:#F#13030#F#abcde#D#1#D#part1#F#part2"));
            CloudUtil.KeyElements ele = CloudUtil.getKeyElements(key);
            System.out.println(ele.dump("TestIt"));
            assertTrue(objectID.getValue().equals(ele.objectID.getValue()));
            if (versionID == null) assertTrue(ele.versionID == null);
            else assertTrue(versionID == ele.versionID);
            if (fileID == null) assertTrue(ele.fileID == null);
            else assertTrue(fileID.equals(ele.fileID));
            assertTrue(true);

        } catch (Exception ex) {
            ex.printStackTrace();
            assertFalse("TestIT exception:" + ex, true);
        }
    }


    @Test
    public void TestManifest()
        throws TException
    {
        TestManifest("Manifest1", "ark:/13030/abcde", true);
        TestManifest("Manifest1", "ark:/13030/abcde", false);
    }
    
    public void TestManifest(
            String header,
            String objectIDS, 
            boolean alphaNumeric)
        throws TException
    {
        try {
            System.out.println("***Manifest" + header + "***");
            Identifier objectID = new Identifier(objectIDS);
            String key = CloudUtil.getManifestKey(objectID, alphaNumeric);
            System.out.println("alpha=" + alphaNumeric);
            System.out.println("eKey=" + key);
            System.out.println("dKey=" + CloudUtil.decodeElement(key));
            CloudUtil.KeyElements ele = CloudUtil.getKeyElements(key);
            System.out.println(ele.dump("TestIt"));
            assertTrue(objectID.getValue().equals(ele.objectID.getValue()));
            assertTrue(ele.versionID == null);
            assertTrue(ele.fileID == null);

        } catch (Exception ex) {
            ex.printStackTrace();
            assertFalse("TestIT exception:" + ex, true);
        }
    }
    
    //@Test (expected=org.cdlib.mrt.utility.TException.INVALID_OR_MISSING_PARM.class)
    public void testException1()
        throws TException
    {
        Identifier id = new Identifier(null, Identifier.Namespace.URL);
    }

    //@Test (expected=org.cdlib.mrt.utility.TException.INVALID_OR_MISSING_PARM.class)
    public void testException2()
        throws TException
    {
        Identifier id = new Identifier("ABCEDF", null);
    }
    
    

    @Test
    public void TestVertical()
        throws TException
    {
        String key = "ark:/13030/m5f212gk|1|abcd|efgh|ijkl";
        CloudUtil.KeyElements ele = CloudUtil.getKeyElements(key);
        System.out.println(ele.dump("TestVertical"));
        assertTrue(ele.fileID.equals("abcd|efgh|ijkl"));
    }
}