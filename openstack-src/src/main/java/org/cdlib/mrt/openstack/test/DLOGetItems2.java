/*
Copyright (c) 2005-2012, Regents of the University of California
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:
 *
- Redistributions of source code must retain the above copyright notice,
  this list of conditions and the following disclaimer.
- Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.
- Neither the name of the University of California nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
OF THE POSSIBILITY OF SUCH DAMAGE.
**********************************************************/
/**
 *
 * @author dloy
 */
package org.cdlib.mrt.openstack.test;
import java.io.File;
import org.cdlib.mrt.cloud.CloudList;
import org.cdlib.mrt.openstack.utility.CloudConst;
import org.cdlib.mrt.openstack.utility.OpenStackCmdAbs;
import org.cdlib.mrt.openstack.utility.OpenStackCmdDLO;
import org.cdlib.mrt.openstack.utility.ResponseValues;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.PropertiesUtil;

public class DLOGetItems2 {
    protected static final String NAME = "DLOGetItems2";
    protected static final String MESSAGE = NAME + ": ";
    
    
    public static void main(String args[])
    {
        try {
            String path = "/replic/MRTMaven/test/s3/mrt-dc.xml";
            String container = "prod-9103";
            
            String key = "ark:/13030/kt6w1023hp|1|system/mrt-dc.xml"; 
            /*
+ark:/13030/kt609nb3jq|1|producer/archival.tif
+ark:/13030/kt109nd8mh|1|producer/FILEID-1.74.36.gif
+ark:/13030/kt538nc6w4|1|system/mrt-dc.xml
+ark:/13030/kt6w1023hp|1|producer/thumbnail.gif
+ark:/13030/kt700018xt|1|system/mrt-ingest.txt
-ark:/13030/ft5d5nb3cg|1|producer/figures/ft5d5nb3cg_00298.gif
-ark:/28722/bk0005c5637|1|producer/mrt-erc.txt
-ark:/13030/vf0ks6ks2|1|system/mrt-erc.txt
-ark:/21198/zz00005btt|1|system/mrt-erc.txt
-ark:/21198/zz00007x95|1|system/mrt-dc.xml
-ark:/21198/zz0001rqcf|1|producer/FID1-VI_81656-A_BVE42183-2.wav
ark:/21198/zz0001vf1r|1|producer/mrt-dc.xml
             */
            //String key = "ark:/13030/kt6w1023hp|1|producer/thumbnail.gif";           
            //String key = "ark:/13030/m5jh41k2|1|producer/WHCF, SMOF - WHC on Food Nutrition and Health.pdf";
            //String key = "ark:/13030/m5jh41k2|1|producer/c8cj8f36.mets.xml";
            //String prefix = "ark";
            OpenStackCmdAbs cmd = new OpenStackCmdDLO("merritt:dloy", 
                    "[0David0]",
                    "https://cloud.sdsc.edu");
                   
            
            get(cmd, container,
                    "ark:/13030/m54f1xq2|1|system%2Fmrt-erc.txt",
                    "/replic/MRTMaven/test/s3/mrt-erc.xml");
            
            get(cmd, container,
                    "ark:/28722/k2ww7bh7j|1|producer/purl.obolibrary.org/obo/UBERON_0006849",
                    "/replic/MRTMaven/test/s3/UBERON_0006849");
            
            get(cmd, container,
                    "ark:/28722/k2n876h61|1|system/mrt-owner.txt",
                    "/replic/MRTMaven/test/s3/mrt-owner.txt");
            
            get(cmd, container,
                    "ark:/28722/k2dn43d7d|1|system/mrt-dc.xml",
                    "/replic/MRTMaven/test/s3/mrt-dc.xml");
            
        } catch (Exception ex) {
            System.out.println("main Exception:" + ex);
            
        }
    }
    
    public static void get(OpenStackCmdAbs cmd, String container, String key, String path)
    {
        System.out.println("**************************************************************");
        System.out.println("***GET:"
                    + " - key=" + key
                    + " - path=" + path
                    + " - container=" + container
                    );
        getList(cmd, container, key);
        System.out.println("---------------------------------------------------------------");
        getItem(cmd, path, container, key);
    }
    
        
    public static void getList(OpenStackCmdAbs cmd, String container, String key)
    {
        try {
            System.out.println("Get metadata");
            CloudList list = cmd.getList(container, key, CloudConst.LONG_TIMEOUT);
            System.out.println(list.dump("Entries"));
            
            
        } catch (Exception ex) {
            System.out.println("main Exception:" + ex);
            
        }
    }
    
    
    
    public static void getItem(OpenStackCmdAbs cmd, String path, String container, String key)
    {
        try {
            System.out.println("Get content");
            File file = new File(path);
            ResponseValues value = cmd.retrieve(container, key, 60000);
            System.out.println(PropertiesUtil.dumpProperties("dump", value.responseProp));
            FileUtil.stream2File(value.inputStream, file);
            
            
        } catch (Exception ex) {
            System.out.println("main Exception:" + ex);
        }
    }
}
