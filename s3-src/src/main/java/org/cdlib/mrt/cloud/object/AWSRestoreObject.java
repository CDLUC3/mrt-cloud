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
public class AWSRestoreObject 
{
    protected static final String NAME = "AWSRestoreObject";
    protected static final String MESSAGE = NAME + ": ";
    //protected static final String testFileS = "/apps/replic/tomcat-28080/webapps/test/big/producer/AS020-VTQ0173-M.mp4";
    protected String bucket = null;
    protected LoggerInf logger = null; 
    protected AWSKeysList awsKeysList = null;
    AWSS3Cloud awsService = AWSS3Cloud.getAWSS3(logger);
    
    public AWSRestoreObject(String bucket, LoggerInf logger)
        throws TException
    {
        this.bucket = bucket;
        this.logger = logger;
        awsKeysList = new AWSKeysList(bucket);
        awsService = AWSS3Cloud.getAWSS3(logger);
    }
    
    public Integer restore(Identifier ark)
        throws TException
    {
        int remainCnt = 0;
        List<String> keys = awsKeysList.process(ark);
        if ((keys == null) || (keys.size() == 0)) {
            System.out.println(MESSAGE + "***KEY NULL***");
            return null;
        }
        try {
            for (String key : keys) {
                CloudResponse response = new CloudResponse();
                boolean restored = awsService.awsRestore(bucket, key, response);
                if (!restored) remainCnt++;
            }
            return remainCnt;
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
}
