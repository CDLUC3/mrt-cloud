/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package org.cdlib.mrt.s3v2.action;

/**
 *
 * @author loy
 */
import java.util.Properties;
import org.cdlib.mrt.cloud.CloudList;
import org.cdlib.mrt.s3.service.CloudResponse;
//import static org.cdlib.mrt.s3v2.action.GetObjectList.awsListAfter;
//import static org.cdlib.mrt.s3v2.test.TestGetObject.getClientAWS;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.PropertiesUtil;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.RestoreRequest;
import software.amazon.awssdk.services.s3.model.GlacierJobParameters;
import software.amazon.awssdk.services.s3.model.RestoreObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.Tier;

/*
 *  For more information about restoring an object, see "Restoring an archived object" at
 *  https://docs.aws.amazon.com/AmazonS3/latest/userguide/restoring-objects.html
 *
 *  Before running this Java V2 code example, set up your development environment, including your credentials.
 *
 *  For more information, see the following documentation topic:
 *
 *  https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/get-started.html
 */
public class RestoreObject {
    public enum RestoreStat{start, inprocess, notfound, complete};
    public static void main(String[] args) 
            throws TException
    {
        
        test_aws_after();
    }
    
    public static void test_aws_after()
        throws TException
    { 
        try {
            System.out.println("***test_aws***");
            String bucketName = "uc3-s3mrt6001-stg";
            //String keyName = "ark:/28722/bk0006w8m0c|1|producer/cabeurle_60_1_00037077.xml";
            //String keyName = "ark:/28722/bk0006w8m0c|1|producer/ark:/28722/bk0006w8m1x";
            String keyName = "ark:/28722/bk0006w8m0c|1|system/mrt-submission-manifest.txt";
            //String keyName = "ark:/28722/bk0006w8m0c|1|producer/cabeurle_60_1_00037077.xxxx2";
	    //String owner = "451826914157";
	    String owner = "913703073800"; //  we can try the merritt-s3 AWS account
            V2Client v2client = V2Client.getAWS();
            S3Client s3Client = v2client.s3Client();
            Properties propbefore = GetObjectMeta.getObjectMeta(s3Client, bucketName, keyName);
            System.out.println(PropertiesUtil.dumpProperties("propbefore", propbefore));
            RestoreObject.RestoreStat stat = RestoreObject.restoreS3Object(s3Client, bucketName, keyName, owner);
            System.out.println(stat.toString());
            Properties prop = GetObjectMeta.getObjectMeta(s3Client, bucketName, keyName);
            System.out.println(PropertiesUtil.dumpProperties("utilstat", prop));
            
        } catch (Exception e) {
            System.out.println("Exception: " + e);
            e.printStackTrace();
        }
        
    }

    /**
     * Restores an S3 object from the Glacier storage class.
     *
     * @param s3                   an instance of the {@link S3Client} to be used for interacting with Amazon S3
     * @param bucketName           the name of the S3 bucket where the object is stored
     * @param keyName              the key (object name) of the S3 object to be restored
     * @param expectedBucketOwner  the AWS account ID of the expected bucket owner
     */
    public static RestoreStat restoreS3Object(S3Client s3, String bucketName, String keyName, String expectedBucketOwner) 
            throws TException
    {
        try {
            RestoreRequest restoreRequest = RestoreRequest.builder()
                .days(10)
                .glacierJobParameters(GlacierJobParameters.builder().tier(Tier.STANDARD).build())
                .build();

            RestoreObjectRequest objectRequest = RestoreObjectRequest.builder()
                .expectedBucketOwner(expectedBucketOwner)
                .bucket(bucketName)
                .key(keyName)
                .restoreRequest(restoreRequest)
                .build();

            s3.restoreObject(objectRequest);
            return RestoreStat.start;

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            if (e.toString().contains("in progress")) {
                return RestoreStat.inprocess;
            } else if (e.toString().contains("404")) {
                return RestoreStat.notfound;
            }
            System.err.println(e.awsErrorDetails().errorMessage());
            throw new TException(e);
            
        }
    }
}
