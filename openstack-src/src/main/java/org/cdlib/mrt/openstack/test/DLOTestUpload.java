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
import org.cdlib.mrt.openstack.utility.OpenStackCmdAbs;
import org.cdlib.mrt.openstack.utility.OpenStackCmdDLO;
import org.cdlib.mrt.openstack.utility.XValues;
import org.cdlib.mrt.openstack.utility.SegmentValues;

import org.cdlib.mrt.openstack.utility.CloudConst;
import org.cdlib.mrt.utility.TException;

public class DLOTestUpload {
    protected static final String NAME = "DLOMidUpload";
    protected static final String MESSAGE = NAME + ": ";
    
    protected String user = null;
    protected String pwd = null;
    protected String host = null;
    protected XValues xValues = null;
    protected String baseName = null;
    protected String container = null;
    protected SegmentValues values = null;
    protected File file = null;
    
    
    public DLOTestUpload(
            String user, String pwd, String host, 
            String container, 
            String baseName,
            File file)
    {
        this.user = user;
        this.pwd = pwd;
        this.host = host;
        this.container = container;
        this.baseName = baseName;;
        this.file = file;
        values = new SegmentValues(container);
    }
    
    public void process()
        throws TException
    {
        InputStream inputStream = null;
        try {
            inputStream = new FileInputStream(file);
            long fileSize = file.length();
            System.out.println("before upload");
            Properties metaProp = new Properties();
            metaProp.setProperty("PROPA", "a");
            metaProp.setProperty("PROPB", "b");
            OpenStackCmdAbs cmd = new OpenStackCmdDLO(user, pwd, host);
            SegmentValues values = cmd.upload(
                    container,
                    baseName,
                    inputStream,
                    fileSize,
                    metaProp,
                    CloudConst.LONG_TIMEOUT);
            System.out.println("TestBig: segCnt=" + values.cnt());
            
            
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
