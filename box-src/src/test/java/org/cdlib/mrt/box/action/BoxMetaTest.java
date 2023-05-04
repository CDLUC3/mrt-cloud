/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.cdlib.mrt.box.action;
import org.cdlib.mrt.box.action.BoxDownload.BoxMeta;
import java.util.Properties;
import org.cdlib.mrt.utility.TFileLogger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.json.JSONObject;

/**
 *
 * @author dloy
 */
public class BoxMetaTest {

    protected static final String NAME = "BoxMetaTest";
    protected static final String MESSAGE = NAME + ": ";
    protected final static String NL = System.getProperty("line.separator");
    protected final static String okJson = "{\"id\":\"911071204179\",\"name\":\"AK-20210502-010102.png\",\"pathName\":\"AK/AK-20210502-010102.png\",\"sha1\":\"5d45c7e788b351c8e46a59c924efd4ed2d9b5f98\",\"size\":342182,\"status\":\"ok\",\"processMs\":1186}";
    public BoxMetaTest() {
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

    
    @Test
    public void testJSONRead()
        throws TException
    {
        try {
            String jsonS = okJson;
            LoggerInf logger = new TFileLogger(NAME, 50, 50);
            JSONObject jobj = new JSONObject(jsonS);
            BoxMeta boxMeta1 = new BoxMeta(jobj);
            Properties prop = boxMeta1.getProp();
            System.out.println(PropertiesUtil.dumpProperties("BoxMeta prop", prop));
            BoxMeta boxMeta2 = new BoxMeta(prop);
            matchBoxMeta(boxMeta1, boxMeta2);


        } catch (Exception ex) {
            System.out.println("Exception:" + ex);
            ex.printStackTrace();
            assertFalse("TestIT exception:" + ex, true);
        }
    }
    
    private static void matchBoxMeta(BoxMeta boxMeta1, BoxMeta boxMeta2)
        throws TException
    {
        try {
            assertTrue("id fails",boxMeta1.getId().equals(boxMeta2.getId()));
            assertTrue("name fails",boxMeta1.getName().equals(boxMeta2.getName()));
            assertTrue("pathName fails",boxMeta1.getPathName().equals(boxMeta2.getPathName()));
            assertTrue("sha1 fails",boxMeta1.getSha1().equals(boxMeta2.getSha1()));
            
            assertTrue("size fails - meta1:" + boxMeta1.getSize() + " - meta2:"  + boxMeta2.getSize(),(boxMeta1.getSize().equals(boxMeta2.getSize())));
            assertTrue("status fails", boxMeta1.getStatus() == boxMeta2.getStatus());
            if (boxMeta1.getProcessMs() != null) {
                assertTrue("processMs fails - meta1:" + boxMeta1.getProcessMs() + " - meta2:"  + boxMeta2.getProcessMs(),(boxMeta1.getProcessMs().equals(boxMeta2.getProcessMs())));
            }


        } catch (Exception ex) {
            System.out.println("Exception:" + ex);
            ex.printStackTrace();
            assertFalse("TestIT exception:" + ex, true);
        }
    }
}