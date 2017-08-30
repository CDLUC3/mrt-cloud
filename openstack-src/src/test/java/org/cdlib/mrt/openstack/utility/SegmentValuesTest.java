/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.cdlib.mrt.openstack.utility;

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
public class SegmentValuesTest {
    public static final boolean ALPHANUMERIC = true;
    public SegmentValuesTest() {
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
    public void Testjson()
        throws TException
    {
        try {
            SegmentValues values = new SegmentValues("mycontainer");
            values.add("aabbcc", 1000, "path/aabbcc");
            values.add("bbccdd", 2000, "path/bbccdd");
            values.add("ccddee", 3000, "path/ccddee");
        System.out.println("JSON:\n" + values.getJson());
            assertTrue(true);

        } catch (Exception ex) {
            ex.printStackTrace();
            assertFalse("TestIT exception:" + ex, true);
        }
    }
    
    @Test
    public void Testxml()
        throws TException
    {
        try {
            SegmentValues values = new SegmentValues("mycontainer");
            values.add("aabbcc", 1000, "path/aabbcc");
            values.add("bbccdd", 2000, "path/bbccdd");
            values.add("ccddee", 3000, "path/ccddee");
        System.out.println("XML:\n" + values.getXML());
            assertTrue(true);

        } catch (Exception ex) {
            ex.printStackTrace();
            assertFalse("TestIT exception:" + ex, true);
        }
    }
}