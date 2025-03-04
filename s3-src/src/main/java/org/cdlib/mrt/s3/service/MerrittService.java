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

package org.cdlib.mrt.s3.service;
import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;      
import java.util.Collection;
import java.util.List;      
import java.util.HashMap;   
import java.util.Set;
import java.util.Properties;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cdlib.mrt.s3.aws.AWSS3Cloud;
import org.cdlib.mrt.s3v2.aws.AWSS3V2Cloud;
import org.cdlib.mrt.s3.cloudhost.CloudhostAPI;
import org.cdlib.mrt.s3.openstack.OpenstackCloud;
import org.cdlib.mrt.s3.pairtree.PairtreeCloud;
import org.cdlib.mrt.s3.service.NodeIO.AccessNode;
import org.cdlib.mrt.s3.store.StoreCloud;
import org.cdlib.mrt.utility.DeleteOnCloseFileInputStream;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;
/**
 *
 * @author DLoy
 * 

 */
public class MerrittService 
{
    
    protected static final String NAME = "NodeIO";
    protected static final String MESSAGE = NAME + ": ";
    private static boolean DEBUG = true; //false;
    private static boolean DEBUG_ACCESS = false;
    
    //public enum ConfigType {jar, file, ssm, yaml};
       
    public static AccessNode getAccessNode(Integer awsVersion, Long nodeNumber, String container, String nodeDescription, Properties cloudProp, LoggerInf logger) 
        throws TException
    {
        CloudStoreInf service = null;
        if (DEBUG_ACCESS) System.out.println("getAccessNode:" 
                + " - nodeNumber=" + nodeNumber
                + " - container=" + container
        );
        String accessMode = null;
        try {
            if (cloudProp == null) {
                throw new TException.INVALID_DATA_FORMAT(MESSAGE + "getService - CloudProp not supplied");
            }
            String serviceType = cloudProp.getProperty("serviceType");
            if (StringUtil.isAllBlank(serviceType)) {
                throw new TException.INVALID_DATA_FORMAT(MESSAGE + "getService - serviceType property required but not found");
            }
            if (serviceType.equals("swift")) {
                service = OpenstackCloud.getOpenstackCloud(cloudProp, logger);
                
            } else if (serviceType.equals("pairtree")) {
                service = PairtreeCloud.getPairtreeCloud(true, logger);
                container = cloudProp.getProperty("base");
                
            } else if (serviceType.equals("store")) {
                String urlS = cloudProp.getProperty("url");
                Integer node = null;
                String nodeS = cloudProp.getProperty("node");
                if (nodeS != null) {
                    node = Integer.parseInt(nodeS);
                    container = "" + node;
                }
                service = StoreCloud.getStoreCloud(urlS, node, logger);
                
            } else if (serviceType.equals("cloudhost")) {
                String urlS = cloudProp.getProperty("base");
                service = CloudhostAPI.getCloudhostAPI(urlS, logger);
                
            } else { // All AWS S3 handling
                if (false) System.out.println("AWS S3 handling:"
                       + " - serviceType:" + serviceType
                       + " - awsVersion:" + awsVersion
                );
                if (awsVersion == null) {
                    throw new TException.INVALID_OR_MISSING_PARM("getAccessNode invald:" + awsVersion);
                }
            
                switch (awsVersion)
                {
                    case 1:
                        return getAWSAccessNodeV1(nodeNumber, container, nodeDescription, cloudProp, logger);

                    case 2:
                        return getAWSAccessNodeV2(nodeNumber, container, nodeDescription, cloudProp, logger);
                }
                throw new TException.INVALID_DATA_FORMAT(MESSAGE + "MerrittService - sawsVersion not found for :" +  awsVersion);
            }
            AccessNode copyNode = new AccessNode(serviceType, accessMode, service, nodeNumber, container, nodeDescription);
            return copyNode;
            
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            System.out.println(MESSAGE + "Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
        
    } 
    
    public static AccessNode getAWSAccessNodeV1(Long nodeNumber, String container, String nodeDescription, Properties cloudProp, LoggerInf logger) 
        throws TException
    {
        CloudStoreInf service = null;
        if (DEBUG_ACCESS) System.out.println("getAccessNode:" 
                + " - nodeNumber=" + nodeNumber
                + " - container=" + container
        );
        String accessMode = null;
        try {
            if (cloudProp == null) {
                throw new TException.INVALID_DATA_FORMAT(MESSAGE + "getService - CloudProp not supplied");
            }
            String serviceType = cloudProp.getProperty("serviceType");
            if (StringUtil.isAllBlank(serviceType)) {
                throw new TException.INVALID_DATA_FORMAT(MESSAGE + "getService - serviceType property required but not found");
            }
            
            if (serviceType.equals("aws")) {
                String storageClassS = cloudProp.getProperty("storageClass");
                if (DEBUG_ACCESS) System.out.println("StorageClassS=" + storageClassS);
                String regionS = cloudProp.getProperty("region");
                accessMode = cloudProp.getProperty("accessMode");
                service = AWSS3Cloud.getAWSS3(logger);
                
            } else if (serviceType.equals("minio")) {
                String accessKey = cloudProp.getProperty("accessKey");
                String secretKey = cloudProp.getProperty("secretKey");
                String endPoint = cloudProp.getProperty("endPoint");
                if (DEBUG_ACCESS) System.out.println("Minio S3"
                        + " - accessKey=" + accessKey
                        + " - secretKey=" + secretKey
                        + " - endPoint=" + endPoint
                );
                service = AWSS3Cloud.getMinio(accessKey, secretKey, endPoint, logger);
                
            } else if (serviceType.equals("sdsc-s3")) {
                String accessKey = cloudProp.getProperty("accessKey");
                String secretKey = cloudProp.getProperty("secretKey");
                String endPoint = cloudProp.getProperty("endPoint");
                if (DEBUG_ACCESS) System.out.println("Minio S3"
                        + " - accessKey=" + accessKey
                        + " - secretKey=" + secretKey
                        + " - endPoint=" + endPoint
                );
                service = AWSS3Cloud.getMinio(
                        accessKey, secretKey, endPoint, logger);
                
            } else if (serviceType.equals("wasabi")) {
                String accessKey = cloudProp.getProperty("accessKey");
                String secretKey = cloudProp.getProperty("secretKey");
                String endPoint = cloudProp.getProperty("endPoint");
                String regionName = cloudProp.getProperty("regionName");
                if (DEBUG_ACCESS) System.out.println("Minio S3"
                        + " - accessKey=" + accessKey
                        + " - secretKey=" + secretKey
                        + " - endPoint=" + endPoint
                        + " - regionName=" + regionName
                );
                service = AWSS3Cloud.getWasabi(accessKey, secretKey, endPoint, regionName, logger);
                
            }
            AccessNode copyNode = new AccessNode(serviceType, accessMode, service, nodeNumber, container, nodeDescription);
            return copyNode;
            
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            System.out.println(MESSAGE + "Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
        
    } 
   
    
    public static AccessNode getAWSAccessNodeV2(Long nodeNumber, String container, String nodeDescription, Properties cloudProp, LoggerInf logger) 
        throws TException
    {
        CloudStoreInf service = null;
        if (DEBUG_ACCESS) System.out.println("getAccessNode:" 
                + " - nodeNumber=" + nodeNumber
                + " - container=" + container
        );
        String accessMode = null;
        try {
            if (cloudProp == null) {
                throw new TException.INVALID_DATA_FORMAT(MESSAGE + "getService - CloudProp not supplied");
            }
            String serviceType = cloudProp.getProperty("serviceType");
            if (StringUtil.isAllBlank(serviceType)) {
                throw new TException.INVALID_DATA_FORMAT(MESSAGE + "getService - serviceType property required but not found");
            }
            if (serviceType.equals("aws")) {
                String storageClassS = cloudProp.getProperty("storageClass");
                if (DEBUG_ACCESS) System.out.println("StorageClassS=" + storageClassS);
                String regionS = cloudProp.getProperty("region");
                accessMode = cloudProp.getProperty("accessMode");
                service = AWSS3V2Cloud.getAWS(logger);
                
            } else if (serviceType.equals("minio")) {
                String accessKey = cloudProp.getProperty("accessKey");
                String secretKey = cloudProp.getProperty("secretKey");
                String endPoint = cloudProp.getProperty("endPoint");
                if (DEBUG_ACCESS) System.out.println("Minio S3"
                        + " - accessKey=" + accessKey
                        + " - secretKey=" + secretKey
                        + " - endPoint=" + endPoint
                );
                service = AWSS3V2Cloud.getMinio(accessKey, secretKey, endPoint, logger);
                
            } else if (serviceType.equals("sdsc-s3")) {
                String accessKey = cloudProp.getProperty("accessKey");
                String secretKey = cloudProp.getProperty("secretKey");
                String endPoint = cloudProp.getProperty("endPoint");
                if (DEBUG_ACCESS) System.out.println("Minio S3"
                        + " - accessKey=" + accessKey
                        + " - secretKey=" + secretKey
                        + " - endPoint=" + endPoint
                );
                service = AWSS3V2Cloud.getSDSC(
                        accessKey, secretKey, endPoint, logger);
                
            } else if (serviceType.equals("wasabi")) {
                String accessKey = cloudProp.getProperty("accessKey");
                String secretKey = cloudProp.getProperty("secretKey");
                String endPoint = cloudProp.getProperty("endPoint");
                String regionName = cloudProp.getProperty("regionName");
                if (DEBUG_ACCESS) System.out.println("Minio S3"
                        + " - accessKey=" + accessKey
                        + " - secretKey=" + secretKey
                        + " - endPoint=" + endPoint
                        + " - regionName=" + regionName
                );
                service = AWSS3V2Cloud.getWasabi(accessKey, secretKey, endPoint, logger);
                
            } else {
                throw new TException.INVALID_DATA_FORMAT(MESSAGE + "getService - serviceType not found for :" +  serviceType);
            }
            AccessNode copyNode = new AccessNode(serviceType, accessMode, service, nodeNumber, container, nodeDescription);
            return copyNode;
            
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            System.out.println(MESSAGE + "Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
        
    }
}
