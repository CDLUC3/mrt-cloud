package org.cdlib.mrt.s3.tools;

/*
Copyright (c) 2005-2010, Regents of the University of California
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
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Properties;

import org.cdlib.mrt.cloud.CloudList;
import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.utility.FixityTests;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFrame;
import org.cdlib.mrt.utility.URLEncoder;
import org.cdlib.mrt.utility.FileUtil;

import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudUtil;
import org.cdlib.mrt.s3.openstack.OpenstackCloud;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TFileLogger;

/**
 * Driver routine for BuildObjectManifest. Not generalized!
 */
public class CreateManifest {
    
    protected static final String NAME = "CreateManifest";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;
    
    public static void main(String[] args) throws IOException {
        /*
         * Important: Be sure to fill in your AWS access credentials in the
         *            AwsCredentials.properties file before you try to run this
         *            sample.
         * http://aws.amazon.com/security-credentials
         */
        main2(args);
    }
    
    
    
    public static void main1(String[] args) throws IOException {
        /*
         * Important: Be sure to fill in your AWS access credentials in the
         *            AwsCredentials.properties file before you try to run this
         *            sample.
         * http://aws.amazon.com/security-credentials
         */
        System.out.println("Run main1");
        TFrame tFrame = null;
        File outFile = null;
        try {
            File directory = new File("/replic/tasks/141023-SDSC-recover/content/m5c82wrx");
            String propertyList[] = {
                "resources/ObjectComponents.properties"};
            tFrame = new TFrame(propertyList, "ObjectCoponents");
            Properties runProp = tFrame.getProperties();
            LoggerInf logger = new TFileLogger("component", 10, 10);
            OpenstackCloud cloud = OpenstackCloud.getOpenstackCloud(runProp, logger);
            BuildObjectManifest bom = BuildObjectManifest.getBuildObjectManifest(
                cloud,
                "prod-9103",
                directory,
                logger);
            //File m50g3r47Man = bom.build("ark:/13030/m50g3r47");
            
            //File m5v98f13Man = bom.build("ark:/13030/m5v98f13");
                    
            //File m5hq4wjqMan = bom.build("ark:/13030/m5hq4wjq"); 
            
            //File m53j3gz9Man = bom.build("ark:/13030/vf2q52fss");
            
            //File zz0000drppMan = bom.build("ark:/21198/zz0000drpp");
            
            //File vf2w37mgqMan = bom.build("ark:/13030/vf2w37mgq");
            
            //File k2513tx4mMan = bom.build("ark:/28722/k2513tx4m");
            
            //File k2rr1rw9hMan = bom.build("ark:/28722/k2rr1rw9h", true, true);
            
            //File k2rr1rw9hMan = bom.build("ark:/28722/k2rr1rw9h", true, true);
            
            File m5c82wrx = bom.build("ark:/13030/m5c82wrx", true, true);
            
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("NAME=" + ex.getClass().getName());
            System.out.println("Exception:" + ex);
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            
        }
    }
    
    
    public static void main2(String[] args) throws IOException {
        /*
         * Important: Be sure to fill in your AWS access credentials in the
         *            AwsCredentials.properties file before you try to run this
         *            sample.
         * http://aws.amazon.com/security-credentials
         */
        System.out.println("Run main1");
        TFrame tFrame = null;
        File outFile = null;
        try {
            //File directory = new File("/replic/tasks/150429-manprob/data");
            File directory = new File("/replic/tasks/150831-fix-manifests/data-replace");
            String propertyList[] = {
                "resources/ObjectComponents.properties"};
            tFrame = new TFrame(propertyList, "ObjectCoponents");
            Properties runProp = tFrame.getProperties();
            LoggerInf logger = new TFileLogger("component", 10, 10);
            OpenstackCloud cloud = OpenstackCloud.getOpenstackCloud(runProp, logger);
            BuildObjectManifest bom = BuildObjectManifest.getBuildObjectManifest(
                cloud,
                "distrib.prod.9001.__",
                directory,
                logger);
            //File m50g3r47Man = bom.build("ark:/13030/m50g3r47");
            
            //File m5v98f13Man = bom.build("ark:/13030/m5v98f13");
                    
            //File m5hq4wjqMan = bom.build("ark:/13030/m5hq4wjq"); 
            
            //File m53j3gz9Man = bom.build("ark:/13030/vf2q52fss");
            
            //File zz0000drppMan = bom.build("ark:/21198/zz0000drpp");
            
            //File vf2w37mgqMan = bom.build("ark:/13030/vf2w37mgq");
            
            //File k2513tx4mMan = bom.build("ark:/28722/k2513tx4m");
            
            //File k2rr1rw9hMan = bom.build("ark:/28722/k2rr1rw9h", true, true);
            
            //File k2rr1rw9hMan = bom.build("ark:/28722/k2rr1rw9h", true, true);
            
            //File m5c82wrx = bom.build("ark:/13030/m5c82wrx", true, true);
            
            
            
           //File m5gf2zh8 = bom.build("ark:/13030/m5gf2zh8", false, false);
           //File bk0005m1w1p = bom.build("ark:/28722/bk0005m1w1p", false, false);
           //File bk0005m8t11 = bom.build("ark:/28722/bk0005m8t11", false, false);
           //File bk0005m6718 = bom.build("ark:/28722/bk0005m6718", false, false);          
            
           //File m5gf2zh8 = bom.build("ark:/13030/m5gf2zh8", true, true);
           File bk0005m1w1p = bom.build("ark:/28722/bk0005m1w1p", true, true);
           File bk0005m8t11 = bom.build("ark:/28722/bk0005m8t11", true, true);
           File bk0005m6718 = bom.build("ark:/28722/bk0005m6718", true, true);
           
            
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("NAME=" + ex.getClass().getName());
            System.out.println("Exception:" + ex);
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            
        }
    }
}
