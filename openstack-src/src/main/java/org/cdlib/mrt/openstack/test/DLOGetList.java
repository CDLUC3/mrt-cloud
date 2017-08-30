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

public class DLOGetList {
    protected static final String NAME = "TestUpdate";
    protected static final String MESSAGE = NAME + ": ";
    
    
    public static void main(String args[])
    {
        try {
            File file = new File("C:/Documents and Settings/dloy/My Documents/tomcat/tomcat-7.0-28080/webapps/test/s3/qt1k67p66s/producer/content/qt1k67p66s.pdf");
            String container = "prod-9103";
            //String prefix = "ark:/13030/m50g3r47";
            //String prefix = "ark:/13030/kt6w1023hp";
            //String prefix = "ark";
            OpenStackCmdAbs cmd = new OpenStackCmdDLO("merritt:dloy", 
                    "[0David0]",
                    "https://cloud.sdsc.edu");
            
            //dump(cmd, container, "ark:/13030/m50g3r47");
            
            //dump(cmd, container, "ark:/13030/m5v98f13");
            
            //dump(cmd, container, "ark:/13030/m5hq4wjq");
            
            dump(cmd, container, "ark:/13030/m5qn64w1");
        } catch (Exception ex) {
            System.out.println("main Exception:" + ex);
            
        }
    }
    
    public static void dump(OpenStackCmdAbs cmd, String container, String prefix)
         throws Exception
    {
        try {
            System.out.println("DUMP"
                    + " - container=" + container
                    + " - prefix=" + prefix
                    );
            CloudList list = cmd.getList(container, prefix, CloudConst.LONG_TIMEOUT);
            System.out.println(list.dump("Entries"));
            
            
        } catch (Exception ex) {
            System.out.println("main Exception:" + ex);
            
        }
        
    }
}
