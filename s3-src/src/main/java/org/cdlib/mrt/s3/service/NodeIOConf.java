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
import java.util.Map;      
import java.util.HashMap;   
import java.util.Set;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.cdlib.mrt.s3.aws.AWSS3Cloud;
import org.cdlib.mrt.s3.cloudhost.CloudhostAPI;
import org.cdlib.mrt.s3.openstack.OpenstackCloud;
import org.cdlib.mrt.s3.pairtree.PairtreeCloud;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import static org.cdlib.mrt.s3.service.NodeIO1.MESSAGE;
import static org.cdlib.mrt.s3.service.NodeIO2.MESSAGE;
import static org.cdlib.mrt.s3.service.NodeIO2.getAccessNode;
import org.cdlib.mrt.s3.store.StoreCloud;
import org.cdlib.mrt.tools.SSMConfigResolver;
import org.cdlib.mrt.tools.YamlParser;
import org.cdlib.mrt.utility.DeleteOnCloseFileInputStream;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;
import org.json.JSONArray;
import org.json.JSONObject;

public class NodeIOConf 
{
    
    protected static final String NAME = "NodeIOConf";
    protected static final String MESSAGE = NAME + ": ";
    private static boolean DEBUG = false;
    private static boolean DEBUG_ACCESS = false;
    private static Pattern pConfig = Pattern.compile("(ssm|file|jar|yaml):[\\s]*([^\\s]*)[\\s]*");
    private static Pattern p2Config = Pattern.compile("([^\\s]*):[\\s]*([^\\s]*)[\\s]*");
    
    protected String nodeName = null;
    protected LoggerInf logger = null;
    protected ConfigType configType = ConfigType.jar;
    
    public enum ConfigType {jar, file, ssm, yaml};
    
    public static void main(String[] args) throws Exception {
        //main_ssm(args);
        main_ssm_default(args);
        //main_file(args);
        //main_jar(args);
    }
    
    public static void main_ssm(String[] args) throws Exception {

        LoggerInf logger = new TFileLogger("lockFile", 10, 10);
        String ssmBase = "SSM:/uc3/mrt/stg/";
        NodeIO nodeIO = NodeIOConf.getNodeIOConfig(ssmBase, logger) ;
        nodeIO.printNodes("main dump");
    } 
    
    public static void main_ssm_default(String[] args) throws Exception {

        LoggerInf logger = new TFileLogger("lockFile", 10, 10);
        String ssmBase = "SSM:";
        NodeIO nodeIO = NodeIOConf.getNodeIOConfig(ssmBase, logger) ;
        nodeIO.printNodes("main dump");
    }
    
    public static void main_file(String[] args) throws Exception {

        LoggerInf logger = new TFileLogger("lockFile", 10, 10);
        String fileBase = ""
                + "file:/apps/replic/tasks/nodeio/200728-newnodeio/nodes";
        NodeIO nodeIO = NodeIOConf.getNodeIOConfig(fileBase, logger) ;
        nodeIO.printNodes("main dump");
    } 
    
    public static void main_jar(String[] args) throws Exception {

        LoggerInf logger = new TFileLogger("lockFile", 10, 10);
        String jarBase = "jar:nodes-stage";
        NodeIO nodeIO = NodeIOConf.getNodeIOConfig(jarBase, logger) ;
    }

    public static NodeIO getNodeIOConfig(LoggerInf logger) 
        throws TException
    {
        
        try {
            ConfigType configType = ConfigType.ssm;
            return getNodeIOSSM(null,  logger);
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            System.out.println(MESSAGE + "Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    public static NodeIO getNodeIOConfig(String config, LoggerInf logger) 
        throws TException
    {
       
        
        ConfigType configType = ConfigType.jar;
        if (StringUtil.isAllBlank(config)) {
            return getNodeIOConfig(logger);
        }
        
        String test = config.toLowerCase();
        String type = "jar";
        String nodeIOName = config;
        Matcher m = pConfig.matcher(test);
        if (m.matches()) {
            type = m.group(1);
        }
        Matcher m2 = p2Config.matcher(config);
        if (m2.matches()) {
            nodeIOName = m2.group(2);
        }
        System.out.println("getNodeIOConfig"
                + " - type:" + type
                + " - nodeIOName:" + nodeIOName
                        );
        configType = ConfigType.valueOf(type);
        try {
            
            switch (configType) {
                case jar:  return getNodeIOJar(nodeIOName, logger);
                case file: return getNodeIOFile(nodeIOName,  logger);
                case ssm: return getNodeIOSSM(nodeIOName,  logger);
                case yaml: return getNodeIOYaml(nodeIOName,  logger);
            default: 
                throw new TException.INVALID_OR_MISSING_PARM("NodeIO configuration switch not found:" + configType);
            }
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            System.out.println(MESSAGE + "Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public static NodeIO getNodeIOJar(String nodeName, LoggerInf logger) 
        throws TException
    {
        
        try {
            
            if (nodeName== null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "nodeName required and missing");
            }
            return new NodeIO(nodeName,logger);
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            System.out.println(MESSAGE + "Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public static NodeIO getNodeIOSSM(String ssmBase, LoggerInf logger) 
        throws TException
    {
        
        try {
            SSMConfigResolver ssm = new SSMConfigResolver(ssmBase);
            
            List<DefNode> defNodes = new ArrayList<DefNode>();
            String jsonS = ssm.getResolvedValue("cloud/nodes/services");
            
                System.out.println("JSONNODES:" + jsonS);
            JSONObject jobj = new JSONObject(jsonS);
            JSONArray jarr = jobj.getJSONArray("nodes");
            
            for (int i=0; i < jarr.length(); i++) {
                JSONObject serviceNode = (JSONObject)jarr.get(i);
                DefNode defNode = getNodeSSM(ssm, serviceNode, logger);
                defNodes.add(defNode);
            }
            NodeIO nodeIO = new NodeIO(defNodes, logger);
            nodeIO.setNodeName("ssm:" + ssmBase);
            return nodeIO;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            System.out.println(MESSAGE + "Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public static DefNode getNodeSSM(SSMConfigResolver ssm, JSONObject serviceNode, LoggerInf logger) 
        throws TException
    {
        
        try {
            
            if (serviceNode == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "ssmNode required and missing");
            }
                long nodeNumber = serviceNode.getLong("id");
                String nodeDescription = serviceNode.getString("desc");
                
                String jsonNodeS = ssm.getResolvedStorageNode(nodeNumber);
                JSONObject jsonObject = new JSONObject((String)jsonNodeS);
                DefNode defNode = jsonDefNode(nodeNumber, nodeDescription, null, jsonObject);
            return defNode;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            System.out.println(MESSAGE + "Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public static NodeIO getNodeIOYaml(String yamlName, LoggerInf logger) 
        throws TException
    {
        try {
            if (DEBUG) System.out.println("Add:" + yamlName);
            String[] parts = yamlName.split("\\s*\\|\\s*");
            if (parts.length < 2) {
                throw new TException.INVALID_OR_MISSING_PARM("getNodeIOYaml requires 2 parts:" + yamlName);
            }
            return getNodeIOYaml(parts[0], parts[1], logger);
            
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            System.out.println(MESSAGE + "Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
        
    }
    
    public static NodeIO getNodeIOYaml(String yamlPath, String envName, LoggerInf logger) 
        throws TException
    {
        
        try {
            SSMConfigResolver ssm = new SSMConfigResolver();
            
            if (StringUtil.isAllBlank(envName)) {
                throw new TException.INVALID_OR_MISSING_PARM("Yaml envName missing");
            }
            List<DefNode> defNodes = new ArrayList<DefNode>();
            SSMConfigResolver ssmResolver = new SSMConfigResolver();
            YamlParser yamlParser = new YamlParser(ssmResolver);
            File fin = new File(yamlPath);
            if (!fin.exists()) {
                throw new TException.INVALID_OR_MISSING_PARM("Yaml file does not exist:" + yamlPath);
            }
            if (DEBUG) {
                String inYml = FileUtil.file2String(fin);
                System.out.println("inYml:\n" + inYml);
            }
            yamlParser.parse(yamlPath);
            yamlParser.resolveValues();
            String jsonout = yamlParser.dumpJson();

            JSONObject jsonBase = new JSONObject(jsonout);
            JSONObject nodestables = jsonBase.getJSONObject("nodes-tables");
            if (DEBUG) System.out.println("nodestables\n" + nodestables.toString());
            JSONArray envTables = nodestables.getJSONArray(envName);
            for (int i=0; i<envTables.length(); i++) {
                JSONObject entry = envTables.optJSONObject(i);
                DefNode defNode = getNodeYaml(entry, logger);
                if (DEBUG)System.out.println(defNode.dump("***DUMP***"));
                defNodes.add(defNode);
                        
            }
            NodeIO nodeIO = new NodeIO(defNodes, logger);
            
            nodeIO.setNodeName("yaml:" + yamlPath + "|" + envName);
            return nodeIO;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            System.out.println(MESSAGE + "Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public static DefNode getNodeYaml(JSONObject yamlEntry, LoggerInf logger) 
        throws TException
    {
        
        try {
            
            if (yamlEntry == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "fileBase required and missing");
            }
           
            long nodeNumber = yamlEntry.getLong("identifier");
            String nodeDescription = null;
            try {
               nodeDescription = yamlEntry.getString("desc");
            } catch (Exception ex) { }
            String bucket = yamlEntry.getString("bucket");
            JSONObject jsonProp = yamlEntry.getJSONObject("service-properties");
            
            DefNode defNode = jsonDefNode(nodeNumber, nodeDescription, bucket, jsonProp);
            return defNode;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            System.out.println(MESSAGE + "Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public static NodeIO getNodeIOFile(String fileBaseS, LoggerInf logger) 
        throws TException
    {
        
        try {
            
            if (fileBaseS== null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "fileBase required and missing");
            }
            
            List<DefNode> defNodes = new ArrayList<DefNode>();
            File fileBase = new File(fileBaseS);
            File keyFile = new File(fileBase, "services");
            String jsonS = FileUtil.file2String(keyFile);
            JSONObject jobj = new JSONObject(jsonS);
            JSONArray jarr = jobj.getJSONArray("nodes");
            
            for (int i=0; i < jarr.length(); i++) {
                JSONObject node = (JSONObject)jarr.get(i);
                long nodeNumber = node.getLong("id");
                String nodeDescription = node.getString("desc");
                DefNode defNode = getNodeFile(fileBase, nodeNumber, nodeDescription, logger);
                defNodes.add(defNode);
            }
            NodeIO nodeIO = new NodeIO(defNodes, logger);
            nodeIO.setNodeName("file:" + fileBaseS);
            return nodeIO;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            System.out.println(MESSAGE + "Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    public static DefNode getNodeFile(File fileBase, Long nodeNumber, String nodeDescription, LoggerInf logger) 
        throws TException
    {
        
        try {
            
            if (fileBase == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "fileBase required and missing");
            }
            if (nodeNumber == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "node required and missing");
            }
            
            File keyFile = new File(fileBase, "" + nodeNumber);
            String jsonS = FileUtil.file2String(keyFile);
            
            JSONObject jsonObject = new JSONObject(jsonS);
            DefNode defNode = jsonDefNode(nodeNumber, nodeDescription, null, jsonObject);
            return defNode;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            System.out.println(MESSAGE + "Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
    }
    
    
    private static DefNode jsonDefNode(Long node, String nodeDescription, String bucket, JSONObject jsonNode)
        throws TException
    {
        try {
            String container = bucket;
            Properties serviceProp = getServiceProp(jsonNode);
            if (container == null) {
                container = serviceProp.getProperty("bucket");
            }
            if (container == null) {
                throw new TException.INVALID_ARCHITECTURE("addMapEntry - no bucket found");
            }
            DefNode defNode = new DefNode(node, container, nodeDescription, serviceProp);
            return defNode;
            
        } catch (TException tex) {
            throw tex;
            
        } catch (Exception ex) {
            System.out.println(MESSAGE + "Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
        
    }
    
     
    private static Properties getServiceProp(JSONObject serviceJSONProp)
    {
        Properties serviceProp = new Properties();
        String [] names = JSONObject.getNames(serviceJSONProp);
        for (String name : names) {
            add(serviceJSONProp, serviceProp, name);
        }
        return serviceProp;
    }
    
    private static Properties add(JSONObject serviceJSONProp, Properties serviceProp, String key)
    {
        try {
            String value = serviceJSONProp.getString(key);
            if (value != null) {
                serviceProp.setProperty(key, value);
            }
            return serviceProp;
            
        } catch (Exception ex) {
            return serviceProp;
        }
    }
}
