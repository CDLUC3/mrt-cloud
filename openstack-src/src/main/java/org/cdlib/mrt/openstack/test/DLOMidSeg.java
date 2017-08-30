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
import java.io.InputStream;
import org.cdlib.mrt.openstack.utility.CloudConst;


import org.cdlib.mrt.utility.MessageDigestValue;
import org.cdlib.mrt.openstack.utility.OpenStackCmdAbs;
import org.cdlib.mrt.openstack.utility.OpenStackCmdDLO;
import org.cdlib.mrt.openstack.utility.ResponseValues;
import org.cdlib.mrt.openstack.utility.XValues;
import org.cdlib.mrt.openstack.utility.SegmentValues;

import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TFileLogger;

public class DLOMidSeg {
    protected static final String NAME = "DLOMidSeg";
    protected static final String MESSAGE = NAME + ": ";
    
    
    public static void main(String args[])
    {
        InputStream inputStream = null;
        try {   
            LoggerInf logger = new TFileLogger("testbig", 50, 50);
            OpenStackCmdDLO cmd = new OpenStackCmdDLO(
                    "merritt:dloy", 
                    "[0David0]", 
                    "https://cloud.sdsc.edu");
            XValues xValues =  cmd.getAuth();
            String container = "big-9102";
            String manName = "seg-test|midB";
            SegmentValues values = new SegmentValues(container);
            if (false) {
                ResponseValues deleteValues = cmd.deleteSingle(
                        xValues, 
                        container,
                        manName,
                        CloudConst.LONG_TIMEOUT);
                System.out.println(deleteValues.dump("deleteValues"));
            }
            if (false) return;
            for (int i=1; i<=4; i++) {
                String name = manName + "/0000" + i;
                ResponseValues responseValues = cmd.getMeta(
                        xValues, 
                        container,
                        name,
                        CloudConst.LONG_TIMEOUT);
                
                String etag = responseValues.getEtag();
                long segSize = responseValues.getSize();
                System.out.println(responseValues.dump("" + i));
                String jsonName = name;
                values.add(etag, segSize, jsonName);
            }
            
            System.out.println("JSON---\n" + values.getJson());
            System.out.println("XML---\n" + values.getXML());
            System.out.println("size---\n" + values.size());
            
            
            ResponseValues responseValues = cmd.retrieve(
                    xValues, 
                    "big-9102",
                    "test|midB",
                    CloudConst.LONG_TIMEOUT);
            inputStream = responseValues.inputStream;
            test("retrieve digest", logger, inputStream);
            
            
        } catch (Exception ex) {
            System.out.println("main Exception:" + ex);
            
        }
    }    
    public static void test(String header, LoggerInf logger, InputStream inputStream)
    {
        try {
            MessageDigestValue fileDigest = new MessageDigestValue(inputStream, "sha-256", logger);
            MessageDigestValue.Result result = fileDigest.getResult();
            dump(header, result);
            
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
    
    public static void dump(String header, MessageDigestValue.Result result)
    {
        
            System.out.println(header + ":"
                    + " - type:" + result.checksumType
                    + " - checksum:" + result.checksum
                    + " - size:" + result.inputSize);
    }
}
