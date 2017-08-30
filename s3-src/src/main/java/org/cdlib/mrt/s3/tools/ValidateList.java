/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cdlib.mrt.s3.tools;
import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.GetObjectRequest;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.service.NodeService;
import org.cdlib.mrt.cloud.CloudList;
import org.cdlib.mrt.cloud.ManifestSAX;
import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.core.MessageDigest;
import org.cdlib.mrt.s3.service.CloudUtil;
import org.cdlib.mrt.s3.tools.CloudManifestCopyNode;
import org.cdlib.mrt.s3.tools.CloudObjectList;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.MessageDigestValue;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;
/**
 * 
 *
 * @author replic
 */
public class ValidateList 
{
    protected static final String NAME = "ValidateList";
    protected static final String MESSAGE = NAME + ": ";
    
    
    protected BufferedReader br = null;
    protected BufferedWriter bw = null;
    protected final LoggerInf logger;
    protected final File listFile;
    protected final String nodeName;
    protected final long [] nodes;
    protected HashMap<Long, Node> map = new HashMap();
                    
    public ValidateList(
            String nodeName,
            File listFile,
            File outFile,
            long [] nodes,
            LoggerInf logger)
        throws TException
    {
         try {
            this.listFile = listFile;
            this.nodeName = nodeName;
            this.logger = logger;
            this.nodes = nodes;
            
            FileInputStream fis = new FileInputStream(this.listFile);
            br = new BufferedReader(new InputStreamReader(fis,
                    Charset.forName("UTF-8")));
            for (long node : nodes) {
                Node nodeObj = new Node(nodeName, node, logger);
                map.put(node, nodeObj);
            }
            FileOutputStream fos = new FileOutputStream(outFile);
            bw = new BufferedWriter(new OutputStreamWriter(fos));
            
        } catch (Exception ex) {
            throw new TException(ex);
            
        }
    }  
    public static void main(String[] args) 
            throws IOException,TException 
    {
        long [] nodes = {910, 3910};
        //String listFileS = "/apps/replic/test/aws/fixdev/5-404arks.txt";
        String listFileS = "/apps/replic/test/store/160908-recover-910/arks.txt";
        File listFile = new File(listFileS);
        String outFileS = "/apps/replic/test/store/160908-recover-910/out.txt";
        File outFile = new File(outFileS);
        LoggerInf logger = new TFileLogger(NAME, 0, 0);
        String nodeName = "nodes-temp-dev";
        try {
            ValidateList vl = new ValidateList(nodeName, listFile,outFile, nodes, logger);
            vl.process();
            
        } catch (TException tex) {
            System.out.println("TException:" + tex);
            tex.printStackTrace();
            
        } catch (Exception ex) {
            System.out.println("TException:" + ex);
            ex.printStackTrace();
            
        }
    }
        
    public void process()
        throws TException
    {
        String line = null;
        try {
            while ((line = br.readLine()) != null) {
                if (StringUtil.isAllBlank(line)) continue;
                if (line.startsWith("#")) continue;
                if (line.length() < 4) continue;
                process(line);
            }
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            throw new TException(ex);
            
        } finally {
             try {
                 br.close();
                 bw.close();
             } catch (Exception ex) { }
        }
        
    }
    
    protected void process(String line)
            throws TException
    {    
        //System.out.println("***Process:" + line);
        try {
            StringBuffer buf = new StringBuffer() ;
            buf.append(">>>" + line + ":");
            for (long node: nodes) {
                boolean present = exists(line, node);
                buf.append(" " + node + "=");
                if (present) {
                    buf.append("yes");
                } else {
                    buf.append("no");
                }
            }
            System.out.println(buf.toString());
            bw.write(buf.toString());
            bw.newLine();
            
        } catch (Exception ex) {
            System.out.println("TException:" + ex);
            ex.printStackTrace();
            throw new TException (ex);
            
        } finally {
            
        }
    }
    
    protected boolean exists(String line, long node) 
    {
        InputStream is = null;
        try {
            Node nodeObj = map.get(node);
            Identifier ark = new Identifier(line);
            is =  nodeObj.service.getManifest(ark);
            if (is == null) return false;
            return true;
            
        } catch (Exception ex) {
            System.out.println("Exception:"
                    + " - ark:" + line
                    + " - node:" + node
                    + " - ex:" + ex
            );
            return false;
            
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (Exception ex) { }
            }
        }
    }
    
    public static class Node
    {
        public long node = 0;
        public NodeService service = null;
        public LoggerInf logger = null;
        public Node(String nodeName, long node, LoggerInf logger) 
                throws TException
        {
            this.node = node;
            this.service = NodeService.getNodeService(nodeName, node, logger);
            this.logger = logger;
        }
    }
    
}
