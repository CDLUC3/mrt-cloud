/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package org.cdlib.mrt.s3.staging.tools;

/**
 *
 * @author loy
 */
import java.io.File;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cdlib.mrt.utility.TException;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.ArrayList;
import org.cdlib.mrt.s3.staging.service.RetrieveResponse;
import org.cdlib.mrt.s3.staging.action.StreamToS3;
import org.cdlib.mrt.s3v2.action.V2Client;
//import static org.cdlib.mrt.s3v2.test.TestGetUrl2S3.get_aws_bigNS;
import software.amazon.awssdk.services.s3.S3Client;
/**
 * Before running this Java V2 code example, set up your development
 * environment, including your credentials.
 * <p>
 * For more information, see the following documentation topic:
 * <p>
 * https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/get-started.html
 */

public class UploadS3Zip {
    protected static final Logger log4j = LogManager.getLogger(); 
    protected static final long defaultNode = 8501;
    //protected CloudStoreInf service = null;
    //protected String bucket = null;
    //protected File containerFile = null;
    //protected String prefix = null;
    //protected int count = 0;
    //protected int deleteCount = 0;
    protected ArrayList<RetrieveResponse> retrieveArr = new ArrayList<>();
    protected S3Client s3Client = null;
    protected String bucketName = null;
    protected String [] digestTypesS = new String [1];
    

    public UploadS3Zip(
            S3Client s3Client,
            String bucketName,
            String [] digestTypesS)
        throws TException
    {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.digestTypesS = digestTypesS;
    }
    
    public static void main(String[] args) 
            throws TException
    {
        V2Client v2client = V2Client.getAWS();
        S3Client s3Client = v2client.s3Client();
        String bucketName = "dloy-tagging-bucket";
        String [] digestTypes = {"sha256","md5"};
        UploadS3Zip uploadS3Zip = new UploadS3Zip(s3Client, bucketName, digestTypes);
        File zipFile = new File("/home/loy/tasks/inventory/250604-sla/tmp/test.zip");
        uploadS3Zip.uploadZip("ziptest", zipFile);
    }
    
    public void uploadZip(String prefix, File zipFile) 
        throws TException
    {
        try (ZipInputStream zipIn = new ZipInputStream(new java.io.FileInputStream(zipFile))) {
           uploadZip(prefix, zipIn);
           
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public void uploadZip(String prefix, InputStream inStream) 
        throws TException
    {
        try (ZipInputStream zipIn = new ZipInputStream(inStream)) {
           uploadZip(prefix, zipIn);
           
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public void uploadZip(String prefix, ZipInputStream zipIn) 
        throws TException
    {
        try (zipIn) {
            ZipEntry entry = zipIn.getNextEntry();
            while (entry != null) {
                String keyPath = prefix + File.separator + entry.getName();
                if (!entry.isDirectory()) {
                    uploadEntry(zipIn, keyPath);
                }
                zipIn.closeEntry();
                entry = zipIn.getNextEntry();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
 
    private void uploadEntry(ZipInputStream zipIn, String keyPath) 
            throws TException 
    {
        System.out.println("keyPath=" + keyPath);
        if (false) return;
        try {
            StreamToS3 streamToS3 = StreamToS3.getStreamToS3 (s3Client, bucketName);
            RetrieveResponse retrieveResponse = streamToS3.uploadFileParts(zipIn, keyPath, digestTypesS);
            streamToS3.addDigestTags(retrieveResponse.getKey(), retrieveResponse);
            retrieveArr.add(retrieveResponse);
            setArr(retrieveResponse);
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
    protected void setArr(RetrieveResponse retrieveResponse) 
    {
        System.out.println("ENTRY:" 
                + " - key:" + retrieveResponse.getKey() 
                + " - length:" + retrieveResponse.length()
                + " - sha256:" + retrieveResponse.getSha256()
        );
    }

    public ArrayList<RetrieveResponse> getRetrieveArr() {
        return retrieveArr;
    }
    
    
}