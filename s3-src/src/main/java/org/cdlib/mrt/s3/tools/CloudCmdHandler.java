package org.cdlib.mrt.s3.tools;

/*
 * Copyright 2010-2011 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Properties;
import java.util.UUID;
import org.cdlib.mrt.cloud.CloudList;
import org.cdlib.mrt.s3.openstack.OpenstackCloud;
import org.cdlib.mrt.s3.pairtree.PairtreeCloud;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.FixityTests;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.FileUtil;

/**

 */
public class CloudCmdHandler {
    
    protected static final String NAME = "CloudCmdHandler";
    protected static final String MESSAGE = NAME + ": ";
    
    public static enum CmdTypes{put, get, delete, list};
    
    
    protected CloudStoreInf service = null;
    protected LoggerInf logger = null;
                    
    public CloudCmdHandler(
            CloudStoreInf service,
            LoggerInf logger)
        throws TException
    {
        this.service = service;
        this.logger = logger;
        if (service == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "service required");
        }
        if (service == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "logger required");
        }
        
    }
    
    public CloudResponse cmd(CmdTypes cmdType, String container, String key, File file)
        throws TException
    {
        try {
            CloudResponse response = null;
            if (cmdType == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "cmdType null");
            }
            switch (cmdType) {
                case put:  
                    response = put(container, key, file);
                    return response;
                    
                case get:  
                    response = get(container, key, file);
                    return response;
                    
                case delete:  
                    response = delete(container, key);
                    return response;
                    
                case list:
                    if (file == null) {
                        response = list(container, key);
                        return response;
                        
                    } else {
                        response = list(container, key, file);
                        return response;
                    }
                    
                default:
                    throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "cmdType not supported:" + cmdType.toString());
            }
            
        } catch (TException tex) {
            System.out.println("TException:" + tex);
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            System.out.println("TException:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
        
    }
    
    public CloudResponse put(String container, String key, File file)
        throws TException
    {
        CloudResponse response = null;
        try {
            response = service.putObject(container, key, file);
            return response;
            
        } catch (TException tex) {
            System.out.println("TException:" + tex);
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            System.out.println("TException:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
        
    }
    
    public CloudResponse get(String container, String key, File file)
        throws TException
    {
        CloudResponse response = new CloudResponse(container, key);
        try {
            InputStream inStream = service.getObject(container, key, response);
            FileUtil.stream2File(inStream, file);
            return response;
            
        } catch (TException tex) {
            System.out.println("TException:" + tex);
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            System.out.println("TException:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
        
    }
    
    public CloudResponse delete(String container, String key)
        throws TException
    {
        try {
            CloudResponse response = service.deleteObject(container, key);
            return response;
            
        } catch (TException tex) {
            System.out.println("TException:" + tex);
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            System.out.println("TException:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
        
    }
    
    public CloudResponse list(String container, String key)
        throws TException
    {
        try {
            CloudResponse response = service.getObjectList(container, key);
            
            return response;
            
        } catch (TException tex) {
            System.out.println("TException:" + tex);
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            System.out.println("TException:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
        
    }
    
    public CloudResponse list(String container, String key, File file)
        throws TException
    {
        try {
            CloudResponse response = service.getObjectList(container, key);
            //List list = response.getObjectList();
            return response;
            
        } catch (TException tex) {
            System.out.println("TException:" + tex);
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            System.out.println("TException:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
        
    }
    
    public static class Test {
        public String val = "val";
    }
}
