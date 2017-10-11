/*
Copyright (c) 2005-2018, Regents of the University of California
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
*********************************************************************/
package org.cdlib.mrt.s3.tools;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.NodeService;
import org.cdlib.mrt.utility.FixityTests;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;
/**
 * 
 *
 * @author replic
 */
public class TestStorageClass 
{
    protected static final String NAME = "TestStorageClass";
    protected static final String MESSAGE = NAME + ": ";
    //protected static final String testFileS = "/apps/replic/tomcat-28080/webapps/test/big/producer/AS020-VTQ0173-M.mp4";
 
    
    protected String nodeName = null;
    protected LoggerInf logger = null;
    protected NodeService service = null;
    
    public TestStorageClass(String nodeName, int node, LoggerInf logger)
        throws TException
    {
        this.nodeName = nodeName;
        this.logger = logger;
        this.service = NodeService.getNodeService(nodeName, node, logger);
    }
    
    
    public void process(String [] keys)
        throws TException
    {
        try {
            System.out.println("Process"
                +   " - bucket:" + service.getBucket()
                +   " - size:" + keys.length
            );
            for (String key : keys) {
                Properties prop = service.getObjectMeta(key);
                System.out.println(PropertiesUtil.dumpProperties(key, prop));
            }
        
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    public static void main(String[] args) 
            throws IOException,TException 
    {
        LoggerInf logger = new TFileLogger(NAME, 50, 50);
        String NODE_NAME = "nodes-prod-class";
        int NODE = 4001;
        TestStorageClass test = new TestStorageClass(NODE_NAME, NODE, logger);
        String [] keys = {
            "ark:/13030/qt2zw5t11r|4|producer/content/supp/CardiffF868.S3C3access.wav",
            "ark:/13030/qt6tx194sd|3|producer/content/supp/HWyckoffKF373.W9A35access.wav",
            "ark:/13030/qt6jr5f1pp|4|producer/content/supp/HWyckoffKF373.W9A35access.wav",
            "ark:/13030/m5k695dk|1|producer/udfr.tar.gz",
            "ark:/28722/k2s75j51d|1|system/mrt-erc.txt"
        };
        try {
            test.process(keys);
                
        } catch (Exception ex) {
            System.out.println(">>>Exception:" + ex) ;
        }
    }
}

