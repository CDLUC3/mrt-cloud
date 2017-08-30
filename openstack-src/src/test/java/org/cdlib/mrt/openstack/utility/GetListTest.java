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
import org.cdlib.mrt.openstack.utility.OpenStackCmdDLO;
import org.cdlib.mrt.cloud.CloudList;
import org.cdlib.mrt.utility.TException;
/**
 *
 * @author dloy
 */
public class GetListTest {
    public static final boolean ALPHANUMERIC = true;
    public GetListTest() {
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
    public void Testxml()
        throws TException
    {
        try {
            String xml = ""
                + "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                + "<container name=\"dpr-9101\">"
                + "<object>"
                + "<name>ark:/13030/kt0000320p|1|producer/FILEID-1.112.31.tif</name>"
                + "<hash>f6bbe764761993836e2af5458ecac5d0</hash>"
                + "<bytes>12189316</bytes>"
                + "<content_type>application/octet-stream</content_type>"
                + "<last_modified>2013-03-26T19:18:41.724450</last_modified>"
                + "</object>"
                + "<object>"
                + "<name>ark:/13030/kt0000320p|1|producer/FILEID-1.112.32.jpg</name>"
                + "<hash>bb31fb3507fe7c96962ab94c86a9560b</hash>"
                + "<bytes>383988</bytes>"
                + "<content_type>application/octet-stream</content_type>"
                + "<last_modified>2013-03-26T19:18:56.698870</last_modified></object>"
                + "<object><name>ark:/13030/kt0000320p|1|producer/FILEID-1.112.36.gif</name>"
                + "<hash>dc46b71787601abb6a0705b4078e08f3</hash>"
                + "<bytes>27868</bytes>"
                + "<content_type>application/octet-stream</content_type>"
                + "<last_modified>2013-03-26T19:19:00.265670</last_modified>"
                + "</object>"
                + "</container>";
            //String lastModified = get(xml, 0, "last_modified");
            //System.out.println("last_modified=" + lastModified);
            CloudList list = getList(xml, "myContainer");
            System.out.println(list.dump("dump"));
            assertTrue(true);

        } catch (Exception ex) {
            ex.printStackTrace();
            assertFalse("TestIT exception:" + ex, true);
        }
    }
    
    public static CloudList getList(String xml, String container)
    {
        CloudList list = new CloudList();
        int pos=0;
        int current=0;
        while (true) {
            pos = xml.indexOf("<object>", current);
            if (pos < 0) break;
            String name = get(xml, pos, "name");
            String hash = get(xml, pos, "hash");
            Long size = getLong(xml, pos, "bytes");
            String contentType = get(xml, pos, "content_type");
            String lastModified = get(xml, pos, "lastModified");
            list.add(container, name, size, hash, contentType, lastModified);
            current = pos + 1;
            System.out.println("curent=" + current);
        }
        return list;
    }
    
    public static String get(String xml, int pos, String key)
    {
        System.out.println("get: - pos=" + pos + " - key=" + key);
        int start = xml.indexOf("<" + key + ">", pos);
        if (start < 0) return null;
        start += key.length() + 2;
        int end = xml.indexOf("</" + key + ">", pos);
        String sub = xml.substring(start, end);
        System.out.println("get: - sub=" + sub);
        return sub;
    }
    
    public static Long getLong(String xml, int pos, String key)
    {
        String sizeS = get(xml, pos, key);
        if (sizeS == null) return null;
        return Long.parseLong(sizeS);
    }
}