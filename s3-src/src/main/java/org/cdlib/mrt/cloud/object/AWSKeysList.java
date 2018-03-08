package org.cdlib.mrt.cloud.object;


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
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.StorageClass;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import org.cdlib.mrt.utility.PropertiesUtil;

import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.cloud.CloudProperties;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.aws.AWSS3Cloud;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.FixityTests;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;
/**
 * 
 *
 * @author replic
 */
public class AWSKeysList 
{
    protected static final String NAME = "AWSKeysList";
    protected static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = false;
    //protected static final String testFileS = "/apps/replic/tomcat-28080/webapps/test/big/producer/AS020-VTQ0173-M.mp4";
    protected String bucket = null;
    protected LoggerInf logger = null; 
    protected AmazonS3 s3Client = null;
    
    public AWSKeysList(String bucket)
        throws TException
    {
        this.bucket = bucket;
        s3Client = new AmazonS3Client(new InstanceProfileCredentialsProvider());
    }
    
    public List<String> process(Identifier ark)
        throws TException
    {
        ArrayList<String> keys = new ArrayList<>();
        if (ark == null) {
            throw new TException.INVALID_OR_MISSING_PARM("ARK missing");
        }
        try {
            String listPrefix = ark.getValue();
            return awsKeyList (s3Client, bucket, listPrefix, keys, logger);
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    public static List<String> awsKeyList (
            AmazonS3 s3Client,
            String bucket,
            String listPrefix,
            List<String> keys,
            LoggerInf logger)
        throws TException
    {
        
        try {
            if (DEBUG) System.out.println(MESSAGE  + "awsKeyList"
                    + " - bucket:" + bucket
                    + " - listPrefix:" + listPrefix
            );
            ObjectListing list = s3Client.listObjects( bucket, listPrefix);

            do {
                List<S3ObjectSummary> summaries = list.getObjectSummaries();
                for (S3ObjectSummary summary : summaries) {
                    String key = summary.getKey();
                    keys.add(key);
                }
                list = s3Client.listNextBatchOfObjects(list);

            } while (list.isTruncated());
            return keys;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    } 
    
    public void dump(String header, List<String> keys) 
    {
        System.out.println(MESSAGE + header);
        for (String key: keys) {
            System.out.println("key:" + key);
        }
    }
    
    public static void main(String[] args) 
            throws IOException,TException 
    {
        LoggerInf logger = new TFileLogger(NAME, 50, 50);
        try {
            Identifier ark = new Identifier("ark:/99999/fk4rx9wmm");
            String bucket = "uc3-s3mrt5001-dev";
            AWSKeysList awsKeyList = new AWSKeysList(bucket);
            List<String> keys = awsKeyList.process(ark);
            awsKeyList.dump("TEST", keys);
                
        } catch (Exception ex) {
            System.out.println(">>>Exception:" + ex) ;
        }
    }

    public AmazonS3 getS3Client() {
        return s3Client;
    }
}
