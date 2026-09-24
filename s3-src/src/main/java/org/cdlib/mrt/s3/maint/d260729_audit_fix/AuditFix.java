/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cdlib.mrt.s3.maint.d260729_audit_fix;

import java.util.ArrayList;
import java.util.Properties;
import java.io.File;
import org.cdlib.mrt.cloud.object.StateHandler;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TFileLogger;

import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.s3.test.TestCloudChecksum;
import org.cdlib.mrt.utility.Checksums;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.TException;

public class AuditFix {
    public static String manifestKey = "ark:/13030/m533239c|manifest";
    public static String oldManifestFile = "/home/loy/s3/minio/260720-misskeys/manifest/oldman.xml";
    public static String oldSha256 = "2b5d9b86a14a4b461efcdc95fe7f4cf557db26f84ff0f03dc94674e7f33f2c4b";
    public static long oldLen = 100621521;
    
    
    //public static String newManifestFile = "/home/loy/s3/minio/260720-misskeys/manifest/mannew.xml";
    //public static String newSha256 = "542ee541cdbd4f4af7afa4f94202250c4eea80d5c11ac6b10dff50a9f8b6067b";
    //public static long newLen = 100621292;
    
    public static String newManifestFile = "/home/loy/s3/minio/260720-misskeys/manifest/mannew.xml";
    public static String newSha256 = "eda6f10e779ac41fb139010c99fb5fc86fdc1f9dc66f70ccc79c5e8ca5e9494d";
    public static long newLen = 100621304;
    
    
    public static NodeIO.AccessNode n9501 = null;
    public static NodeIO.AccessNode n7502 = null;
            
    private static LoggerInf logger = new TFileLogger("test", 50, 50);
       //String key = "ark:/13030/m53z94nj|manifest"; // hang - now works
    public static void main(String[] argv) {
        try {
            String yamlName = "jar:nodes-remote";
            long node = 9501;
            NodeIO nodeIO = NodeIO.getNodeIOConfig(yamlName, logger) ;
            n9501 = nodeIO.getAccessNode(9501);
            n7502 = nodeIO.getAccessNode(7502);
    
            if (false) {
                Properties prop = getStats(n9501, "ark:/13030/m533239c|manifest");
                if (false) return;
                String sha256 = prop.getProperty("sha256");
                Long size = Long.getLong(prop.getProperty("size"), null);
                File contentFile = getContent(n9501, "ark:/13030/m533239c|manifest", "/home/loy/s3/minio/260720-misskeys/test/downtest.xml");
                //testFiles("test", contentFile, oldLen, oldSha256);
              
            }
            
            // uploadFile(n7502, "/home/loy/s3/minio/260720-misskeys/manifest/oldman.xml", "ark:/13030/m533239c|save_manifest");
            // validateMeta(n7502, "ark:/13030/m533239c|save_manifest", oldLen, oldSha256);
            
            //upload(n7502, "/home/loy/s3/minio/260720-misskeys/manifest/mannew.xml", "ark:/13030/m533239c|manifest", newLen, newSha256);
            //testFiles();
            //testContents();
           // boolean notPresent = removeContent(n7502, "ark:/13030/m533239c|save_manifest");
            //System.out.println("removed1:" + notPresent);
            //notPresent = removeContent(n7502, "ark:/13030/m533239c|manifest");
            //System.out.println("removed2:" + notPresent);
            //testn7502();
            prodFlip9501();
            
        } catch (Exception ex) {
                // TODO Auto-generated catch block
                System.out.println("Exception:" + ex);
                ex.printStackTrace();
        }
    }
    
    protected static void prodFlip9501()
    {
        System.out.println("####> prodFlip9501");
        try {
            remove(n9501, manifestKey);
            upload(n9501, newManifestFile, manifestKey, newLen, newSha256);
            
        } catch (Exception ex) {
            System.out.println("testn7502 exception:" + ex);
            ex.printStackTrace();
        }
    }
    
    protected static void testn7502()
    {
        System.out.println("## testn7502");
        try {
            upload(n7502, oldManifestFile, manifestKey, oldLen, oldSha256);
            remove(n7502, manifestKey);
            upload(n7502, newManifestFile, manifestKey, newLen, newSha256);
            remove(n7502, manifestKey);
            
        } catch (Exception ex) {
            System.out.println("testn7502 exception:" + ex);
            ex.printStackTrace();
        }
    }
    
    protected static void testFiles()
    {
        try {
            xTestFile("old-old-old", new File(oldManifestFile), oldLen, oldSha256);
            xTestFile("new-new-new", new File(oldManifestFile), newLen, newSha256);
            xTestFile("old-new-old", new File(oldManifestFile), newLen, oldSha256);
            xTestFile("old-old-new", new File(oldManifestFile), oldLen, newSha256);
        } catch (Exception ex) {
            System.out.println("testFile Exception:" + ex);
        }
    }
    
    protected static void testContents()
    {
        try {
            xTestContent("old-old-old", n9501, manifestKey, oldLen, oldSha256);
            xTestContent("old-old-new", n9501, manifestKey, oldLen, newSha256);
            xTestContent("old-old-new", n9501, manifestKey, newLen, oldSha256);
            xTestContent("n7502-old-old", n7502, "ark:/13030/m533239c|save_manifest", oldLen, oldSha256);
            xTestContent("n7502-new-new", n7502, manifestKey, newLen, newSha256);
        } catch (Exception ex) {
            System.out.println("testFile Exception:" + ex);
        }
    }
    
    protected static void xTestFile(String header, File testFile, long testLen, String testSha256)
    {
        boolean retVal = false;
        try {
            retVal = testFile(testFile, testLen, testSha256);
            System.out.println("***" + header + " - retval:" + retVal
                    );
            
        } catch (Exception ex) {
            System.out.println("*** Exception:" + header + "***\n"
                    + " - retval:" + retVal
                    );
        }
    }   
    
    
    
    protected static void xTestContent(String header, NodeIO.AccessNode accessNode, String testKey, long testLen, String testSha256)
    {
        boolean retVal = false;
        try {
            retVal = testContent(accessNode, testKey, testLen, testSha256);
            System.out.println("***xTestContent(" + header + ") - retval:" + retVal
                    );
            
        } catch (Exception ex) {
            System.out.println("*** Exception:" + header + "***\n"
                    + " - retval:" + retVal
                    );
        }
    }   
    
    public static boolean upload(NodeIO.AccessNode accessNode, 
            String upfilePath, 
            String uploadKey,
            long testLen, 
            String testSha256)
        throws TException
    {
        try {
        
            File uploadFile = new File(upfilePath);
            if (!testFile(uploadFile, testLen, testSha256)) {
                throw new TException.INVALID_DATA_FORMAT("Invalid file content:" + upfilePath);
            }
            uploadFile(accessNode, uploadFile, uploadKey);
            if (!testContent(accessNode, uploadKey, testLen, testSha256)) {
                throw new TException.INVALID_DATA_FORMAT("Invalid upload content:" 
                         + " - bucket:" + accessNode.container
                         + " - key:" + uploadKey
                );
            }
            String [] types = {"sha256"};
            TestCloudChecksum.testValid(accessNode.nodeNumber, types, accessNode.service, accessNode.container, uploadKey, testSha256, testLen, logger);
            System.out.println("### upload success:"
                        + " - node:" + accessNode.container
                        + " - bucket:" + accessNode.container
                        + " - manifestKey:" + uploadKey
                        + " - upfilePath:" + upfilePath
            );
            return true;
            
        } catch (TException tex) {
                // TODO Auto-generated catch block
                System.out.println("Exception:" + tex);
                throw tex;
                
        } catch (Exception ex) {
                // TODO Auto-generated catch block
                System.out.println("Exception:" + ex);
                throw new TException(ex);
        }
    }
     
     
    public static Properties getStats(NodeIO.AccessNode accessNode, String key) {

    	try {
            
            CloudStoreInf service = accessNode.service;
            String bucket = accessNode.container;
            Properties prop = service.getObjectMeta(bucket, key);
            System.out.println(PropertiesUtil.dumpProperties(key, prop));
            return prop;
    
        } catch (Exception ex) {
                // TODO Auto-generated catch block
                System.out.println("Exception:" + ex);
                ex.printStackTrace();
                return null; 
        }
    }
     
    public static void uploadFile(NodeIO.AccessNode accessNode, File uploadFile, String uploadKey) {

    	try {
            
            System.out.println("uploadFile:"
                    + " - node:" + accessNode.nodeNumber
                    + " - upfilePath:" + uploadFile.getAbsolutePath()
                    + " - uploadKey:" + uploadKey
            );
            CloudStoreInf service = accessNode.service;
            String bucket = accessNode.container;
            CloudResponse response = service.putObject(bucket, uploadKey, uploadFile);
            if (response.getException() != null) {
                throw response.getException();
            }
            getStats(accessNode, uploadKey);
            
        } catch (Exception ex) {
                // TODO Auto-generated catch block
                System.out.println("Exception:" + ex);
                ex.printStackTrace();
        }
    }
     
     public static File getContent(NodeIO.AccessNode accessNode, String downloadKey, String downfilePath) {

    	try {
            System.out.println("getContent:"
                    + " - node:" + accessNode.nodeNumber
                    + " - downloadKey:" + downloadKey
                    + " - downfilePath:" + downfilePath
            );
            CloudStoreInf service = accessNode.service;
            String bucket = accessNode.container;
            File downloadFile = new File(downfilePath);
            CloudResponse response = CloudResponse.get(bucket, downloadKey);
            service.getObject(bucket, downloadKey, downloadFile, response);
            if (response.getException() != null) {
                throw response.getException();
            }
            if (!downloadFile.exists()) {
                throw new TException.REQUESTED_ITEM_NOT_FOUND("File does not exist:" + downloadFile.getCanonicalPath());
            }
            return downloadFile;
    
        } catch (Exception ex) {
                // TODO Auto-generated catch block
                System.out.println("Exception:" + ex);
                ex.printStackTrace();
                return null;
        }
    }        
     
    public static void remove(NodeIO.AccessNode accessNode, String removeKey) 
        throws Exception
    {

    	try {
            boolean notPresent = removeContent(accessNode, removeKey);
            if (!notPresent) { // exists
                throw new TException.INVALID_ARCHITECTURE("Content not removed:" 
                        + " - bucket:" + accessNode.container
                        + " - manifestKey:" + removeKey
                );
            } else {
                System.out.println("### removeContent success:"
                        + " - bucket:" + accessNode.container
                        + " - manifestKey:" + removeKey
                );
            }
    
        } catch (TException tex) {
                // TODO Auto-generated catch block
                System.out.println("Exception:" + tex);
                tex.printStackTrace();
                throw tex;
                        
        } catch (Exception ex) {
                // TODO Auto-generated catch block
                System.out.println("Exception:" + ex);
                ex.printStackTrace();
                throw new TException(ex);
        }
    }  
     
    public static boolean removeContent(NodeIO.AccessNode accessNode, String removeKey) {

    	try {
            CloudStoreInf service = accessNode.service;
            String bucket = accessNode.container;
            System.out.println("removeContent:"
                    + " - node:" + accessNode.nodeNumber
                    + " - bucket:" + bucket
                    + " - downloadKey:" + removeKey
            );
            Properties metaProp = getStats(accessNode, removeKey);
            if ((metaProp == null) || metaProp.isEmpty()) {
                System.out.println("Object not found for removal:" 
                        + " - bucket:" + bucket
                        + " - removeKey:" + removeKey
                );
                return true;
            }
            System.out.println(PropertiesUtil.dumpProperties("removeContent before", metaProp));
            CloudResponse response = service.deleteObject(bucket, removeKey);
            if (response.getException() != null) {
                throw response.getException();
            }
            Properties metaPropAfter = getStats(accessNode, removeKey);
            if ((metaPropAfter == null) || metaPropAfter.isEmpty()) {
                System.out.println("Object removed:" 
                        + " - bucket:" + bucket
                        + " - removeKey:" + removeKey
                );
                return true;
            }
            System.out.println(PropertiesUtil.dumpProperties("removeContent Object not removed", metaProp));
            return false;
    
        } catch (Exception ex) {
                // TODO Auto-generated catch block
                System.out.println("Exception:" + ex);
                ex.printStackTrace();
                return false;
        }
    }
     
    protected static boolean testFile(File testFile, long testLen, String testSha256)
    {
        boolean match = true;
        System.out.println("*** testFile: " + testFile.getAbsolutePath()
                + " - testLen=" + testLen
                + " - testSha256=" + testSha256
        );
        try {
            if ((testFile == null) || !testFile.exists()) {
                System.out.println("File does not exist");
                return false;
            }
            if (testFile.length() != testLen) {
                System.out.println("MISMATCH Length - file:" + testFile.length() + "passed length:" + testLen);
                return false;
            }
            
            String [] types = {"sha256"};
            
            Checksums checksums = Checksums.getChecksums(types, testFile);
            String fileChecksum = checksums.getChecksum("sha256");
            if ((fileChecksum == null) || fileChecksum.isEmpty()) {
                System.out.println("Checksum does not exist");
                return false;
            }
            
            if (!fileChecksum.equals(testSha256)) {
                System.out.println("sha256Fails:\n"
                        + "  File sha256:" + fileChecksum + "\n"
                        + "  Test sha256:" + testSha256 + "\n"
                );
                return false;
            }
                 
                
            System.out.println("VALIDATE MATCH: len:" + testFile.length()
                    + " - sha256:" + fileChecksum
                    );
            return true;
            
        } catch (Exception ex) {
            System.out.println("validate Exception:" + ex);
            return false;
        }
        
    }
     
    protected static boolean testContent(NodeIO.AccessNode accessNode, String testKey, long testLen, String testSha256)
    {
        boolean match = true;
        try {
            CloudStoreInf service = accessNode.service;
            String bucket = accessNode.container;
            System.out.println("*** validateMeta: " 
                    + " - bucket:" + bucket
                    + " - downloadKey:" + testKey
                    + " - testLen:" + testLen
                    + " - testSha256:" + testSha256
            );
            Properties metaProp = getStats(accessNode, testKey);
            if ((metaProp == null) || metaProp.isEmpty()) {
                return false;
            }
            System.out.println(PropertiesUtil.dumpProperties(testKey, metaProp));
            String metaSha256 = metaProp.getProperty("sha256");
            if (metaSha256 == null) return false;
            if (!metaSha256.equals(testSha256)) {
                System.out.println("fails size:"
                        + " - testSha256=" + testSha256
                        + " - metaSha256=" + metaSha256
                );
                return false;
            }
            String sizeS = metaProp.getProperty("size");
            Long propLen = Long.parseLong(sizeS);
            if (propLen != testLen) {
                System.out.println("fails size:"
                        + " - testLen=" + testLen
                        + " - propLen=" + propLen
                );
                return false;        
            }
            System.out.println("*** validateMeta MATCH: " 
                    + " - bucket:" + bucket
                    + " - testKey:" + testKey
                    + " - propLen:" + propLen
                    + " - metaSha256:" + metaSha256
            );
            String [] types = {"sha256"};
            TestCloudChecksum.testValid(accessNode.nodeNumber, types, accessNode.service, accessNode.container, testKey, testSha256, testLen, logger);
            return true;
            
        } catch (Exception ex) {
            System.out.println("validate Exception:" + ex);
            return false;
        }
        
    }
}