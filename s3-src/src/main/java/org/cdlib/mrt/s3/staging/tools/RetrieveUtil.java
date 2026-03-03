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

public class RetrieveUtil {
    protected static final Logger log4j = LogManager.getLogger(); 

    
}