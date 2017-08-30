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

public class DLOGetListFull {
    protected static final String NAME = "DLOGetListFull";
    protected static final String MESSAGE = NAME + ": ";
    
    
    public static void main(String args[])
    {
        try {
            //File file = new File("C:/Documents and Settings/dloy/My Documents/tomcat/tomcat-7.0-28080/webapps/test/s3/qt1k67p66s/producer/content/qt1k67p66s.pdf");
            String container = "uc3a.bucket";
            String ark = "ark:/99999/fk4dr3bzf";
            //String prefix = "ark";
            OpenStackCmdAbs cmd = new OpenStackCmdDLO("cdltemp:dloy", 
                    "0David0",
                    "https://cloud.sdsc.edu");


            System.out.println("before getList");
            String marker = null;
            for (int i=0; i < 200; i++) {
                System.out.println("***DLOGetListFull:"
                        + " - ark=" + ark
                        + " - marker=" + marker
                );
                CloudList list = cmd.getListFull(container, ark, marker, 5, CloudConst.LONG_TIMEOUT);
                //CloudList list = cmd.getListFull(container, ark, 20, marker, CloudConst.LONG_TIMEOUT);
                if (list == null) break;
                System.out.println(list.dump("Entries"));
                CloudList.CloudEntry entry = cmd.getLastEntry(list);
                if (entry == null) break;
                marker = entry.getKey();
            }
            
            
        } catch (Exception ex) {
            System.out.println("main Exception:" + ex);
            
        }
    }
}
