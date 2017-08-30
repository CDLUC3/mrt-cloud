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
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;


import org.cdlib.mrt.cloud.CloudProperties;
import org.cdlib.mrt.openstack.utility.OpenStackCmdAbs;
import org.cdlib.mrt.openstack.utility.OpenStackCmdDLO;
import org.cdlib.mrt.openstack.utility.ResponseValues;
import org.cdlib.mrt.openstack.utility.CloudConst;
import org.cdlib.mrt.openstack.utility.SegmentValues;
import org.cdlib.mrt.utility.TException;

public class DLOSmallUpload {
    protected static final String NAME = "TestUpdate";
    protected static final String MESSAGE = NAME + ": ";
    
    
    public static void main(String args[])
    {
        InputStream inputStream = null;
        try {
            File file = new File("C:/Documents and Settings/dloy/My Documents/tomcat/tomcat-7.0-28080/webapps/test/s3/qt1k67p66s/producer/content/qt1k67p66s.pdf");
            String container = "big-9102";
            inputStream = new FileInputStream(file);
            String objectName = "test|small|A1";
            Properties metaProp = new Properties();
            metaProp.setProperty("PROPA", "a");
            metaProp.setProperty("PROPB", "b");
            OpenStackCmdAbs cmd = new OpenStackCmdDLO("merritt:dloy", 
                    "[0David0]",
                    "https://cloud.sdsc.edu");
            
            try {
                System.out.println("before delete");
                SegmentValues values = cmd.delete(
                        container,
                        objectName,
                        CloudConst.LONG_TIMEOUT);
            } catch (Exception ex) {
                System.out.println("no delete:" + ex);
            }
            
            System.out.println("before upload");
            SegmentValues values = cmd.upload(
                    container,
                    objectName,
                    inputStream,
                    null,
                    file.length(),
                    metaProp,
                    CloudConst.LONG_TIMEOUT);
            
            System.out.println("before retrieve");
            ResponseValues responseValues = cmd.retrieve(
                    container,
                    objectName,
                    CloudConst.LONG_TIMEOUT);
            
            System.out.println(responseValues.dump("retrieve"));
            Long size = responseValues.getSize();
            if (size == null) {
                throw new TException.INVALID_OR_MISSING_PARM("size not found");
            }
            String etag = responseValues.getEtag();
            if (etag == null) {
                throw new TException.INVALID_OR_MISSING_PARM("etag not found");
            }
            System.out.println("TestUpdate"
                    + " - size:" + size
                    + " - etag:" + etag
                    );
            
            CloudProperties cloudProp = responseValues.getCloudProperties();
            System.out.println(cloudProp.dump("dump"));
            
        } catch (Exception ex) {
            System.out.println("main Exception:" + ex);
            
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                    System.out.println("Stream closed");
                } catch (Exception ex) { }            
            }
        }
    }
}
